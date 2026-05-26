@Tags(['integration'])
library;

import 'dart:async';

import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_test_server/graphql_ws_test_server.dart';
import 'package:graphql_ws_web_socket_channel_connector/graphql_ws_web_socket_channel_connector.dart';
import 'package:test/test.dart';

void main() {
  late GraphqlWsTestServer server;

  setUp(() async {
    server = await GraphqlWsTestServer.start();
  });

  tearDown(() async {
    await server.dispose();
  });

  Client buildClient({Map<String, Object?>? params}) {
    return createClient(
      url: () => server.uri,
      connector: webSocketChannelConnector,
      connectionParams: params == null ? null : () => params,
      retryAttempts: 0,
    );
  }

  test('connects, subscribes, receives Next then Complete', () async {
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

  test('streams multiple subscription events in order', () async {
    server.register('counter', (payload) async* {
      for (var i = 0; i < 5; i++) {
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

    expect(values, equals([0, 1, 2, 3, 4]));
  });

  test('cancelling the stream subscription stops the server-side stream',
      () async {
    final emitted = Completer<void>();
    var cancelled = false;
    server.register('infinite', (payload) {
      late StreamController<Map<String, Object?>> controller;
      var n = 0;
      Timer? timer;
      controller = StreamController<Map<String, Object?>>(
        onListen: () {
          timer = Timer.periodic(const Duration(milliseconds: 10), (_) {
            controller.add({
              'data': {'n': n++},
            });
            if (!emitted.isCompleted) emitted.complete();
          });
        },
        onCancel: () {
          cancelled = true;
          timer?.cancel();
        },
      );
      return controller.stream;
    });

    final client = buildClient();
    addTearDown(client.dispose);

    final sub = client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription Infinite { n }',
            operationName: 'infinite',
          ),
        )
        .listen((_) {});

    await emitted.future;
    await sub.cancel();

    // The server should observe the client's complete and cancel its stream.
    await _eventually(() => cancelled, label: 'server-side stream cancelled');
  });

  test('client disposes cleanly — server sees socket close', () async {
    server.register('hello', (payload) {
      return Stream.value(<String, Object?>{
        'data': {'hello': 'world'},
      });
    });

    final client = buildClient();
    await client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'query Hello { hello }',
            operationName: 'hello',
          ),
        )
        .single;

    expect(server.connectedClients, equals(1));
    await client.dispose();
    await _eventually(() => server.connectedClients == 0,
        label: 'server sees client disconnect');
  });

  test('server-emitted error reaches stream onError', () async {
    server.register('boom', (payload) {
      return Stream.error([
        {'message': 'something broke'},
      ]);
    });

    final client = buildClient();
    addTearDown(client.dispose);

    final stream = client.stream<Map<String, Object?>, Object?>(
      const SubscribePayload(
        query: 'query Boom { boom }',
        operationName: 'boom',
      ),
    );

    Object? captured;
    try {
      await stream.toList();
    } catch (e) {
      captured = e;
    }
    expect(captured, isA<List<GraphQLFormattedError>>());
    final errors = captured! as List<GraphQLFormattedError>;
    expect(errors, hasLength(1));
    expect(errors.first.message, equals('something broke'));
  });

  test('fatal close code: server sends 4400, adapter surfaces it without retry',
      () async {
    server.onConnectionInit = (_) => 4400;

    final connected = <ConnectedEvent>[];
    final client = createClient(
      url: () => server.uri,
      connector: webSocketChannelConnector,
      retryAttempts: 3,
    );
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

  test('retry exhaustion via adapter: error surfaces after budget exhausted',
      () async {
    var attempts = 0;
    server.onConnectionInit = (_) {
      attempts++;
      return 4403;
    };

    final client = createClient(
      url: () => server.uri,
      connector: webSocketChannelConnector,
      retryAttempts: 2,
      retryWait: (_) => Future<void>.delayed(const Duration(milliseconds: 10)),
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

    expect(attempts, equals(3)); // 1 initial + 2 retries
    expect(captured, isA<LikeCloseEvent>());
    expect((captured! as LikeCloseEvent).code, equals(4403));
  });

  test('connectionParams: server receives payload through web_socket_channel',
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

    final client = createClient(
      url: () => server.uri,
      connector: webSocketChannelConnector,
      retryAttempts: 0,
      connectionParams: () => {'token': 'abc'},
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

    expect(captured, equals({'token': 'abc'}));
  });
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
