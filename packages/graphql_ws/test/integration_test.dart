/// Integration tests against a real graphql-transport-ws server, using the
/// default `DartIoWebSocketAdapter` transport. The point is to verify the
/// adapter works end-to-end — the protocol semantics are exhaustively
/// covered by the unit suite.
@Tags(['integration'])
library;

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

  Client buildClient() {
    return createClient(
      url: () => server.uri,
      retryAttempts: 0,
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
}
