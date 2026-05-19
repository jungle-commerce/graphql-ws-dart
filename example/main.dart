/// Minimal usage example for the `graphql_ws` client.
///
/// Run against your own server, e.g.:
///
///     dart run example/main.dart wss://example.com/graphql
library;

import 'dart:async';
import 'dart:io';

import 'package:graphql_ws/graphql_ws.dart';

Future<void> main(List<String> args) async {
  if (args.isEmpty) {
    stderr.writeln('Usage: dart run example/main.dart <ws-url>');
    exit(64);
  }
  final uri = Uri.parse(args.first);

  final client = createClient(
    url: () => uri,
    // Pass any payload the server expects in ConnectionInit (auth tokens, etc.)
    connectionParams: () => {'authToken': Platform.environment['TOKEN']},
    // 10 second keep-alive ping.
    keepAlive: const Duration(seconds: 10),
    // Tolerate brief network blips.
    retryAttempts: 3,
  );

  // Lifecycle observation.
  client.on<ConnectedEvent>(
      (e) => stderr.writeln('[graphql_ws] connected (recovered=${e.wasRetry})'));
  client.on<ClosedEvent>(
      (e) => stderr.writeln('[graphql_ws] closed ${e.code} ${e.reason ?? ''}'));
  client.on<ErrorEvent>(
      (e) => stderr.writeln('[graphql_ws] error ${e.error}'));

  // Subscribe via Stream — the recommended Dart-idiomatic API.
  final sub = client
      .stream<Map<String, Object?>, Map<String, Object?>>(
        const SubscribePayload(
          query: r'subscription Greetings { greetings }',
        ),
      )
      .listen(
        (result) => stdout.writeln('next: ${result.data}'),
        onError: (Object err) =>
            stderr.writeln('subscription error: $err'),
        onDone: () => stdout.writeln('subscription complete'),
      );

  // Run for 30 seconds, then tear everything down cleanly.
  await Future<void>.delayed(const Duration(seconds: 30));
  await sub.cancel();
  await client.dispose();
}
