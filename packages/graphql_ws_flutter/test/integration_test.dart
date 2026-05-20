/// End-to-end tests: a real `graphql_ws` client, a real server, and a real
/// [GraphqlWsConnectionManager] wired together. The manager's decision logic
/// is covered exhaustively in `connection_manager_test.dart` with a spy —
/// these tests prove the pieces actually compose.
@Tags(['integration'])
library;

import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';
import 'package:graphql_ws_test_server/graphql_ws_test_server.dart';

import 'utils/fake_connectivity_source.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  late GraphqlWsTestServer server;
  late FakeConnectivitySource connectivity;

  setUp(() async {
    server = await GraphqlWsTestServer.start();
    server.register(
        'long', (_) => StreamController<Map<String, Object?>>().stream);
    connectivity = FakeConnectivitySource();
  });

  tearDown(() async {
    await connectivity.dispose();
    await server.dispose();
  });

  /// A connected client with a long-lived subscription. [connects] collects
  /// every `ConnectedEvent`. The subscription swallows errors — these tests
  /// yank the connection around and assert on reconnection, not on stream
  /// health.
  Future<(Client, List<ConnectedEvent>)> connectedClient() async {
    final client = createClient(
      url: () => server.uri,
      retryAttempts: 5,
      retryWait: (_) => Future<void>.delayed(const Duration(milliseconds: 10)),
    );
    addTearDown(client.dispose);

    final connects = <ConnectedEvent>[];
    final first = Completer<void>();
    client.on<ConnectedEvent>((e) {
      connects.add(e);
      if (!first.isCompleted) first.complete();
    });

    final sub = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription { t }',
            operationName: 'long',
          ),
        )
        .listen((_) {}, onError: (Object _) {});
    addTearDown(sub.cancel);

    await first.future.timeout(const Duration(seconds: 2));
    return (client, connects);
  }

  test('resume after a long background reconnects end-to-end', () async {
    final (client, connects) = await connectedClient();
    final manager = GraphqlWsConnectionManager(
      client: client,
      connectivitySource: connectivity,
      trustWindow: const Duration(milliseconds: 10),
      deadAfter: const Duration(milliseconds: 50),
    )..attach();
    addTearDown(manager.dispose);

    manager.didChangeAppLifecycleState(AppLifecycleState.paused);
    await Future<void>.delayed(const Duration(milliseconds: 80));
    manager.didChangeAppLifecycleState(AppLifecycleState.resumed);

    await _eventually(() => connects.length >= 2,
        label: 'client reconnected after resume');
    expect(connects.last.wasRetry, isTrue);
  });

  test('connectivity change reconnects end-to-end', () async {
    final (client, connects) = await connectedClient();
    final manager = GraphqlWsConnectionManager(
      client: client,
      connectivitySource: connectivity,
    )..attach();
    addTearDown(manager.dispose);

    connectivity.emit(true); // e.g. a wifi-to-cellular handoff

    await _eventually(() => connects.length >= 2,
        label: 'client reconnected after connectivity change');
  });
}

Future<void> _eventually(
  bool Function() predicate, {
  Duration timeout = const Duration(seconds: 3),
  Duration step = const Duration(milliseconds: 10),
  String label = 'predicate',
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for: $label');
    }
    await Future<void>.delayed(step);
  }
}
