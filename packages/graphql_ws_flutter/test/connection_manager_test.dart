import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';

import 'utils/fake_connectivity_source.dart';
import 'utils/spy_client.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  late SpyClient client;
  late FakeConnectivitySource connectivity;
  late List<RecoveryTrigger> recoveries;

  setUp(() {
    client = SpyClient();
    connectivity = FakeConnectivitySource();
    recoveries = [];
  });

  tearDown(() async {
    await connectivity.dispose();
  });

  GraphqlWsConnectionManager build({
    Duration trustWindow = const Duration(seconds: 5),
    Duration deadAfter = const Duration(minutes: 3),
    Duration verificationTimeout = const Duration(seconds: 4),
    bool terminateOnPause = false,
  }) {
    final manager = GraphqlWsConnectionManager(
      client: client,
      connectivitySource: connectivity,
      trustWindow: trustWindow,
      deadAfter: deadAfter,
      verificationTimeout: verificationTimeout,
      terminateOnPause: terminateOnPause,
      onRecovery: recoveries.add,
    )..attach();
    addTearDown(manager.dispose);
    return manager;
  }

  group('app lifecycle', () {
    test('resume after a long background terminates immediately', () async {
      final manager = build(
        trustWindow: const Duration(milliseconds: 10),
        deadAfter: const Duration(milliseconds: 50),
      );

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);
      await Future<void>.delayed(const Duration(milliseconds: 70));
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

      expect(client.terminateCount, equals(1));
      expect(client.pings, isEmpty, reason: 'long background skips the probe');
      expect(recoveries, equals([RecoveryTrigger.staleAfterBackground]));
    });

    test('resume after a brief background does nothing', () async {
      final manager = build(trustWindow: const Duration(seconds: 10));

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

      expect(client.terminateCount, isZero);
      expect(client.pings, isEmpty);
      expect(recoveries, isEmpty);
    });

    test('resume after a medium background probes; a pong keeps the socket',
        () async {
      final manager = build(
        trustWindow: Duration.zero,
        deadAfter: const Duration(seconds: 100),
        verificationTimeout: const Duration(milliseconds: 80),
      );

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

      expect(client.pings, hasLength(1), reason: 'a probe ping is sent');
      client.emitPong(); // server is alive

      await Future<void>.delayed(const Duration(milliseconds: 120));
      expect(client.terminateCount, isZero);
      expect(recoveries, isEmpty);
    });

    test('resume after a medium background terminates a frozen socket',
        () async {
      final manager = build(
        trustWindow: Duration.zero,
        deadAfter: const Duration(seconds: 100),
        verificationTimeout: const Duration(milliseconds: 80),
      );

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

      expect(client.pings, hasLength(1));
      // No pong arrives.
      await Future<void>.delayed(const Duration(milliseconds: 120));
      expect(client.terminateCount, equals(1));
      expect(recoveries, equals([RecoveryTrigger.failedLivenessProbe]));
    });

    test('a fresh connection during the probe window counts as alive',
        () async {
      final manager = build(
        trustWindow: Duration.zero,
        deadAfter: const Duration(seconds: 100),
        verificationTimeout: const Duration(milliseconds: 80),
      );

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

      // The client reconnected on its own before the watchdog fired.
      client.emitConnected();

      await Future<void>.delayed(const Duration(milliseconds: 120));
      expect(client.terminateCount, isZero,
          reason: 'a ConnectedEvent proves liveness just like a pong');
      expect(recoveries, isEmpty);
    });

    test('terminateOnPause terminates as the app is paused', () {
      final manager = build(terminateOnPause: true);

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);

      expect(client.terminateCount, equals(1));
      expect(recoveries, equals([RecoveryTrigger.appPaused]));
    });

    test('without terminateOnPause, pausing alone does nothing', () {
      final manager = build();

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);

      expect(client.terminateCount, isZero);
      expect(recoveries, isEmpty);
    });

    test('inactive and detached are ignored', () async {
      final manager = build(trustWindow: Duration.zero);

      manager.didChangeAppLifecycleState(AppLifecycleState.inactive);
      manager.didChangeAppLifecycleState(AppLifecycleState.detached);
      // A resume with no prior paused/hidden must also be a no-op.
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(client.terminateCount, isZero);
      expect(client.pings, isEmpty);
    });
  });

  group('connectivity', () {
    test('an online change terminates so the client reconnects', () async {
      build();

      connectivity.emit(true);
      await pumpEventQueue();

      expect(client.terminateCount, equals(1));
      expect(recoveries, equals([RecoveryTrigger.connectivityChanged]));
    });

    test('an offline change does nothing', () async {
      build();

      connectivity.emit(false);
      await pumpEventQueue();

      expect(client.terminateCount, isZero);
      expect(recoveries, isEmpty);
    });
  });

  group('lifecycle of the manager itself', () {
    test('after dispose, lifecycle and connectivity changes are ignored',
        () async {
      final manager = build(
        trustWindow: Duration.zero,
        deadAfter: const Duration(milliseconds: 1),
      );

      manager.dispose();

      manager.didChangeAppLifecycleState(AppLifecycleState.paused);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      manager.didChangeAppLifecycleState(AppLifecycleState.resumed);
      connectivity.emit(true);
      await pumpEventQueue();

      expect(client.terminateCount, isZero);
      expect(recoveries, isEmpty);
    });

    test('attach and dispose are idempotent', () {
      final manager = GraphqlWsConnectionManager(
        client: client,
        connectivitySource: connectivity,
      );
      manager.attach();
      manager.attach(); // second call is a no-op
      manager.dispose();
      manager.dispose(); // second call is a no-op
    });
  });
}
