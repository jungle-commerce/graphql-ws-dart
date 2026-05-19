/// Minimum-viable smoke tests to validate the client/server harness wiring
/// before porting the full client.test.ts suite. If these don't pass the rest
/// of the port is on quicksand.
@Tags(['smoke'])
library;

import 'package:graphql_ws/graphql_ws.dart';
import 'package:test/test.dart';

import 'utils/deferred.dart';
import 'utils/tserver.dart';
import 'utils/tsub.dart';

void main() {
  late TServer server;

  setUp(() async {
    server = await TServer.start();
  });

  tearDown(() async {
    await server.dispose();
  });

  test('lazy: connects on first subscribe, sends ConnectionInit + Subscribe',
      () async {
    final client = createClient(url: () => server.uri);

    final sub = TSubscribe.start<Map<String, Object?>, Map<String, Object?>>(
      client,
      const SubscribePayload(query: '{ value }'),
    );

    final tclient = await server.waitForClient();
    final init = await tclient.waitForMessage();
    expect(init, isA<ConnectionInitMessage>());

    tclient.sendMessage(const ConnectionAckMessage());

    final subscribe = await tclient.waitForSubscribe();
    expect(subscribe.payload.query, equals('{ value }'));

    sub.dispose();
    await client.dispose();
  });

  test('non-lazy: connects immediately', () async {
    final connected = Deferred<void>();
    final client = createClient(
      url: () => server.uri,
      lazy: false,
      onNonLazyError: (_) {},
      on: {
        ConnectedEvent: ((ClientEvent event) {
          if (event is ConnectedEvent) connected.resolve();
        }),
      },
    );

    final tclient = await server.waitForClient();
    final init = await tclient.waitForMessage();
    expect(init, isA<ConnectionInitMessage>());
    tclient.sendMessage(const ConnectionAckMessage());

    await connected.future.timeout(const Duration(seconds: 2));
    await client.dispose();
  });

  test('delivers Next messages to the sink and completes', () async {
    final client = createClient(url: () => server.uri);

    final sub = TSubscribe.start<Map<String, Object?>, Map<String, Object?>>(
      client,
      const SubscribePayload(query: 'subscription { tick }'),
    );

    final tclient = await server.waitForClient();
    await tclient.waitForMessageOfType(MessageType.connectionInit);
    tclient.sendMessage(const ConnectionAckMessage());
    final subscribe = await tclient.waitForSubscribe();

    tclient.sendMessage(NextMessage(
      id: subscribe.id,
      payload: FormattedExecutionResult<Map<String, Object?>,
          Map<String, Object?>>(data: {'tick': 1}),
    ));
    tclient.sendMessage(NextMessage(
      id: subscribe.id,
      payload: FormattedExecutionResult<Map<String, Object?>,
          Map<String, Object?>>(data: {'tick': 2}),
    ));
    tclient.sendMessage(CompleteMessage(id: subscribe.id));

    final r1 = await sub.waitForNext();
    expect(r1!.data, equals({'tick': 1}));

    final r2 = await sub.waitForNext();
    expect(r2!.data, equals({'tick': 2}));

    expect(await sub.waitForComplete(), isTrue);

    await client.dispose();
  });

  test('delivers Error to the sink and does not complete', () async {
    final client = createClient(url: () => server.uri);

    final sub = TSubscribe.start<Map<String, Object?>, Map<String, Object?>>(
      client,
      const SubscribePayload(query: '{ bad }'),
    );

    final tclient = await server.waitForClient();
    await tclient.waitForMessageOfType(MessageType.connectionInit);
    tclient.sendMessage(const ConnectionAckMessage());
    final subscribe = await tclient.waitForSubscribe();

    tclient.sendMessage(ErrorMessage(
      id: subscribe.id,
      payload: const [GraphQLFormattedError(message: 'boom')],
    ));

    final err = await sub.waitForError();
    expect(err, isA<List<GraphQLFormattedError>>());
    expect((err! as List<GraphQLFormattedError>).first.message, equals('boom'));

    // Should NOT complete after error.
    expect(await sub.waitForComplete(timeout: const Duration(milliseconds: 50)),
        isFalse);

    await client.dispose();
  });
}
