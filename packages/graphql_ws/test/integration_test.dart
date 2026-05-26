/// Integration tests against a real graphql-transport-ws server, using the
/// default `DartIoWebSocketAdapter` transport. The point is to verify the
/// adapter works end-to-end — the protocol semantics are exhaustively
/// covered by the unit suite.
@Tags(['integration'])
library;

import 'dart:async';

import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_test_server/graphql_ws_test_server.dart';
import 'package:test/test.dart';

void main() {
  late GraphqlWsTestServer server;

  setUp(() async {
    server = await GraphqlWsTestServer.start();
  });

  tearDown(() async {
    await server.dispose();
  });

  Client buildClient({
    int retryAttempts = 0,
    Duration keepAlive = Duration.zero,
    Duration lazyCloseTimeout = Duration.zero,
    FutureOr<Map<String, Object?>?> Function()? connectionParams,
    bool lazy = true,
  }) {
    return createClient(
      url: () => server.uri,
      retryAttempts: retryAttempts,
      keepAlive: keepAlive,
      lazyCloseTimeout: lazyCloseTimeout,
      connectionParams: connectionParams,
      lazy: lazy,
      retryWait: (_) => Future<void>.delayed(const Duration(milliseconds: 10)),
    );
  }

  test('query: connects, receives Next, then Complete', () async {
    server.register('hello', (payload) {
      return Stream.value(<String, Object?>{
        'data': {'hello': 'world'},
      });
    });

    final client = buildClient();
    addTearDown(client.dispose);

    final result = await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'query Hello { hello }',
            operationName: 'hello',
          ),
        )
        .single;

    expect(result.data, equals({'hello': 'world'}));
  });

  test('subscription: streams multiple Next messages in order', () async {
    server.register('counter', (payload) async* {
      for (var i = 0; i < 3; i++) {
        yield <String, Object?>{
          'data': {'count': i},
        };
      }
    });

    final client = buildClient();
    addTearDown(client.dispose);

    final values = await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription Counter { count }',
            operationName: 'counter',
          ),
        )
        .map((r) => (r.data?['count']! as int))
        .toList();

    expect(values, equals([0, 1, 2]));
  });

  test('server-emitted error reaches stream onError', () async {
    server.register('boom', (payload) {
      return Stream.error([
        {'message': 'kaboom'},
      ]);
    });

    final client = buildClient();
    addTearDown(client.dispose);

    Object? captured;
    try {
      await client
          .stream<Map<String, Object?>, Object?>(
            const SubscribePayload(
              query: 'query Boom { boom }',
              operationName: 'boom',
            ),
          )
          .toList();
    } catch (e) {
      captured = e;
    }
    expect(captured, isA<List<GraphQLFormattedError>>());
    expect((captured! as List<GraphQLFormattedError>).first.message,
        equals('kaboom'));
  });

  test('keep-alive: client sends Ping, observes Pong from server', () async {
    server.register('long', (payload) => _neverEnding());

    final client = buildClient(keepAlive: const Duration(milliseconds: 50));
    addTearDown(client.dispose);

    final pingSent = Completer<void>();
    final pongReceived = Completer<void>();
    client.on<PingEvent>((e) {
      if (!e.received && !pingSent.isCompleted) pingSent.complete();
    });
    client.on<PongEvent>((e) {
      if (e.received && !pongReceived.isCompleted) pongReceived.complete();
    });

    // Keep a subscription open so the connection stays up.
    final sub = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription Long { tick }',
            operationName: 'long',
          ),
        )
        .listen((_) {});
    addTearDown(sub.cancel);

    await pingSent.future.timeout(const Duration(seconds: 2));
    await pongReceived.future.timeout(const Duration(seconds: 2));
  });

  test('connectionParams: server receives the payload in connection_init',
      () async {
    Map<String, Object?>? captured;
    server.onConnectionInit = (payload) {
      captured = payload;
      return null;
    };
    server.register('hello', (payload) {
      return Stream.value(<String, Object?>{
        'data': {'hello': 'world'}
      });
    });

    final client = buildClient(
      connectionParams: () => {'authToken': 'shibboleth'},
    );
    addTearDown(client.dispose);

    await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'query Hello { hello }',
            operationName: 'hello',
          ),
        )
        .single;

    expect(captured, equals({'authToken': 'shibboleth'}));
  });

  test('retry: reconnects and resumes subscription after server kills socket',
      () async {
    var subscribeCount = 0;
    server.register('counter', (payload) async* {
      subscribeCount++;
      for (var i = 0; i < 100; i++) {
        yield <String, Object?>{
          'data': {'count': i},
        };
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
    });

    final client = buildClient(retryAttempts: 3);
    addTearDown(client.dispose);

    final connections = <ConnectedEvent>[];
    client.on<ConnectedEvent>(connections.add);

    final received = <int>[];
    final got5 = Completer<void>();
    final got5AfterRetry = Completer<void>();
    final sub = client
        .stream<Map<String, Object?>, Object?>(
      const SubscribePayload(
        query: 'subscription Counter { count }',
        operationName: 'counter',
      ),
    )
        .listen((r) {
      received.add(r.data!['count']! as int);
      if (received.length == 5 && !got5.isCompleted) got5.complete();
      if (connections.length >= 2 && !got5AfterRetry.isCompleted) {
        got5AfterRetry.complete();
      }
    });
    addTearDown(sub.cancel);

    await got5.future.timeout(const Duration(seconds: 2));
    await server.killActiveConnections();
    await got5AfterRetry.future.timeout(const Duration(seconds: 2));

    expect(connections.length, greaterThanOrEqualTo(2),
        reason: 'client should have reconnected at least once');
    expect(connections.last.wasRetry, isTrue);
    expect(subscribeCount, greaterThanOrEqualTo(2),
        reason: 'subscription should have been re-issued after reconnect');
  });

  test('concurrent: multiple subscriptions on one socket are independent',
      () async {
    server.register('a', (payload) async* {
      for (var i = 0; i < 3; i++) {
        yield <String, Object?>{
          'data': {'src': 'a', 'n': i},
        };
      }
    });
    server.register('b', (payload) async* {
      for (var i = 0; i < 3; i++) {
        yield <String, Object?>{
          'data': {'src': 'b', 'n': i + 100},
        };
      }
    });

    final client = buildClient();
    addTearDown(client.dispose);

    final aFuture = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(query: 's', operationName: 'a'),
        )
        .toList();
    final bFuture = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(query: 's', operationName: 'b'),
        )
        .toList();

    final aResults = await aFuture;
    final bResults = await bFuture;

    expect(aResults.map((r) => r.data!['n']).toList(), equals([0, 1, 2]));
    expect(bResults.map((r) => r.data!['n']).toList(), equals([100, 101, 102]));
    expect(server.connectedClients, equals(1),
        reason: 'both subscriptions should share one socket');
  });

  test('lazy: connects on first subscribe, disconnects after last unsubscribe',
      () async {
    server.register('hello', (payload) {
      return Stream.value(<String, Object?>{
        'data': {'hello': 'world'}
      });
    });

    final client = buildClient();
    addTearDown(client.dispose);

    expect(server.connectedClients, equals(0),
        reason: 'no socket should exist before first subscribe');

    await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'query Hello { hello }',
            operationName: 'hello',
          ),
        )
        .single;

    await _eventually(() => server.connectedClients == 0,
        label: 'socket closes after last unsubscribe');
  });

  test('connectionAckWaitTimeout: client closes 4504 when server acks too late',
      () async {
    // Server delays ack by 300 ms; client gives up after 50 ms.
    server.onConnectionInit = (payload) async {
      await Future<void>.delayed(const Duration(milliseconds: 300));
      return null;
    };

    final client = createClient(
      url: () => server.uri,
      retryAttempts: 0,
      connectionAckWaitTimeout: const Duration(milliseconds: 50),
    );
    addTearDown(client.dispose);

    Object? captured;
    try {
      await client
          .stream<Map<String, Object?>, Object?>(
            const SubscribePayload(query: 'query { x }', operationName: 'x'),
          )
          .toList();
    } catch (e) {
      captured = e;
    }

    expect(captured, isA<LikeCloseEvent>());
    expect((captured! as LikeCloseEvent).code, equals(4504));
  });

  test('fatal close code: server sends 4400, client does not retry', () async {
    // 4400 is hardcoded fatal — client should surface immediately even with
    // retryAttempts budget remaining.
    server.onConnectionInit = (_) => 4400;

    final connected = <ConnectedEvent>[];
    final client = buildClient(retryAttempts: 3);
    addTearDown(client.dispose);
    client.on<ConnectedEvent>(connected.add);

    Object? captured;
    try {
      await client
          .stream<Map<String, Object?>, Object?>(
            const SubscribePayload(query: 'query { x }', operationName: 'x'),
          )
          .toList();
    } catch (e) {
      captured = e;
    }

    expect(captured, isA<LikeCloseEvent>());
    expect((captured! as LikeCloseEvent).code, equals(4400));
    expect(connected, isEmpty, reason: 'must not retry a fatal close code');
  });

  test('non-lazy + onNonLazyError: error surfaces without a subscriber',
      () async {
    server.onConnectionInit = (_) => 4403;

    final errors = <Object>[];
    final client = createClient(
      url: () => server.uri,
      lazy: false,
      retryAttempts: 0,
      onNonLazyError: errors.add,
    );
    addTearDown(client.dispose);

    await _eventually(() => errors.isNotEmpty, label: 'onNonLazyError fires');
    expect(errors.first, isA<LikeCloseEvent>());
    expect((errors.first as LikeCloseEvent).code, equals(4403));
  });

  test('retry exhaustion: non-fatal close surfaces after budget runs out',
      () async {
    // 4403 (forbidden) is non-fatal — the client will retry until the budget
    // is exhausted, then surface the last error to subscribers.
    var attempts = 0;
    server.onConnectionInit = (_) {
      attempts++;
      return 4403;
    };

    final client = buildClient(retryAttempts: 2);
    addTearDown(client.dispose);

    Object? captured;
    try {
      await client
          .stream<Map<String, Object?>, Object?>(
            const SubscribePayload(query: 'query { x }', operationName: 'x'),
          )
          .toList();
    } catch (e) {
      captured = e;
    }

    // 1 initial attempt + 2 retries = 3 total.
    expect(attempts, equals(3));
    expect(captured, isA<LikeCloseEvent>());
    expect((captured! as LikeCloseEvent).code, equals(4403));
  });

  test('lazyCloseTimeout: socket stays open during debounce window', () async {
    server.register('hello', (payload) {
      return Stream.value(<String, Object?>{
        'data': {'hello': 'world'}
      });
    });

    final client = buildClient(
      lazyCloseTimeout: const Duration(milliseconds: 150),
    );
    addTearDown(client.dispose);

    await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'query Hello { hello }',
            operationName: 'hello',
          ),
        )
        .single;

    // Immediately after the subscription ends the socket should still be open
    // (debounce window hasn't expired).
    expect(server.connectedClients, equals(1),
        reason: 'socket must stay alive during lazyCloseTimeout window');

    // After the window expires the socket should close.
    await _eventually(() => server.connectedClients == 0,
        timeout: const Duration(seconds: 1),
        label: 'socket closes after lazyCloseTimeout');
  });

  test('shouldRetry override: custom predicate stops retry immediately',
      () async {
    var attempts = 0;
    server.onConnectionInit = (_) {
      attempts++;
      return 4403; // non-fatal by default, but we'll refuse all retries
    };

    final client = createClient(
      url: () => server.uri,
      retryAttempts: 5,
      retryWait: (_) => Future<void>.delayed(const Duration(milliseconds: 10)),
      shouldRetry: (_) => false,
    );
    addTearDown(client.dispose);

    Object? captured;
    try {
      await client
          .stream<Map<String, Object?>, Object?>(
            const SubscribePayload(query: 'query { x }', operationName: 'x'),
          )
          .toList();
    } catch (e) {
      captured = e;
    }

    expect(attempts, equals(1),
        reason: 'shouldRetry: false must prevent any retry');
    expect(captured, isA<LikeCloseEvent>());
  });

  test('server ping → client pongs by default', () async {
    server.register('long', (payload) => _neverEnding());

    final client = buildClient();
    addTearDown(client.dispose);

    final connected = Completer<void>();
    client.on<ConnectedEvent>((_) {
      if (!connected.isCompleted) connected.complete();
    });

    final sub = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription { tick }',
            operationName: 'long',
          ),
        )
        .listen((_) {});
    addTearDown(sub.cancel);

    await connected.future.timeout(const Duration(seconds: 2));
    server.pingAllClients();

    await _eventually(() => server.receivedPongCount >= 1,
        label: 'client sent pong in response to server ping');
  });

  test('disablePong: client stays silent when server pings', () async {
    server.register('long', (payload) => _neverEnding());

    final client = createClient(
      url: () => server.uri,
      retryAttempts: 0,
      disablePong: true,
    );
    addTearDown(client.dispose);

    final connected = Completer<void>();
    client.on<ConnectedEvent>((_) {
      if (!connected.isCompleted) connected.complete();
    });

    final sub = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription { tick }',
            operationName: 'long',
          ),
        )
        .listen((_) {});
    addTearDown(sub.cancel);

    await connected.future.timeout(const Duration(seconds: 2));
    server.pingAllClients();

    // Give it time to arrive and respond (or not).
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(server.receivedPongCount, equals(0),
        reason: 'disablePong must suppress the automatic pong reply');
  });

  test('ping(): on-demand ping draws a Pong back from the server', () async {
    server.register('long', (payload) => _neverEnding());

    final client = buildClient();
    addTearDown(client.dispose);

    final connected = Completer<void>();
    client.on<ConnectedEvent>((_) {
      if (!connected.isCompleted) connected.complete();
    });
    final pongReceived = Completer<void>();
    client.on<PongEvent>((e) {
      if (e.received && !pongReceived.isCompleted) pongReceived.complete();
    });

    final sub = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription { tick }',
            operationName: 'long',
          ),
        )
        .listen((_) {});
    addTearDown(sub.cancel);

    await connected.future.timeout(const Duration(seconds: 2));
    expect(server.receivedPongCount, equals(0));

    client.ping();
    await pongReceived.future.timeout(const Duration(seconds: 2));
  });

  test('generateId: custom id generator is used for subscription frames',
      () async {
    server.register('hello', (payload) {
      return Stream.value(<String, Object?>{
        'data': {'hello': 'world'}
      });
    });

    final receivedIds = <String>[];
    server.onSubscribe = (id, payload) => receivedIds.add(id);

    const customId = 'my-custom-id-42';
    final client = createClient(
      url: () => server.uri,
      retryAttempts: 0,
      generateId: (_) => customId,
    );
    addTearDown(client.dispose);

    await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'query Hello { hello }',
            operationName: 'hello',
          ),
        )
        .single;

    expect(receivedIds, equals([customId]));
  });
}

/// A subscription stream that never emits values but never completes either.
/// Useful for tests that need a long-lived connection (e.g. keep-alive).
Stream<Map<String, Object?>> _neverEnding() {
  final controller = StreamController<Map<String, Object?>>();
  // Do not add anything; do not close. Cancelled when the subscriber cancels.
  return controller.stream;
}

Future<void> _eventually(
  bool Function() predicate, {
  Duration timeout = const Duration(seconds: 2),
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
