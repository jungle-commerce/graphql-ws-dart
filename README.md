# graphql_ws

Coherent, zero-dependency, lazy, simple GraphQL over WebSocket Protocol compliant **client** for Dart and Flutter.

A faithful port of the JavaScript [`graphql-ws`](https://github.com/enisdenjo/graphql-ws) client. Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md) (`graphql-transport-ws` sub-protocol).

- Zero external dependencies.
- 131 tests covering protocol validation, lifecycle, retries, lazy/keepalive, ping/pong, terminate, and streaming.
- Native transport via `dart:io.WebSocket`; Flutter web + custom transports via a 60-line adapter.

## Install

```yaml
dependencies:
  graphql_ws: ^0.1.0
```

## Quick start

```dart
import 'package:graphql_ws/graphql_ws.dart';

final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  connectionParams: () => {'authToken': 'my-token'},
);

final sub = client
    .stream<Map<String, Object?>, Map<String, Object?>>(
      const SubscribePayload(query: 'subscription { greetings }'),
    )
    .listen(
      (result) => print(result.data),
      onError: (Object err) => print('error: $err'),
      onDone: () => print('done'),
    );

// later:
await sub.cancel();
await client.dispose();
```

## Features

- Single connection, multiplexed subscriptions
- Lazy connect / lazy close (with optional calmdown debounce)
- Connection acknowledgement timeout (close code 4504)
- Configurable keep-alive (ping/pong)
- Automatic reconnection with randomised exponential backoff
- Custom `retryWait` / `shouldRetry` strategies
- Lifecycle hooks: `ConnectingEvent`, `OpenedEvent`, `ConnectedEvent`, `MessageEvent`, `PingEvent`, `PongEvent`, `ClosedEvent`, `ErrorEvent` (sealed-class hierarchy for pattern matching)
- `terminate()` for force-closing stuck sockets
- Custom JSON reviver for inbound message customisation
- Custom subscription id generator
- Pluggable `WebSocketConnector` for any transport

## API at a glance

```dart
abstract interface class Client implements Disposable {
  // Listen to lifecycle events (sealed-class pattern matching).
  void Function() on<E extends ClientEvent>(void Function(E) listener);

  // Low-level subscribe with a Sink<T> (1:1 with the JS client).
  void Function() subscribe<TData, TExtensions>(
    SubscribePayload payload,
    Sink<FormattedExecutionResult<TData, TExtensions>> sink,
  );

  // Dart-idiomatic subscribe as a Stream.
  Stream<FormattedExecutionResult<TData, TExtensions>>
      stream<TData, TExtensions>(SubscribePayload payload);

  Future<void> dispose();
  void terminate();
}
```

The full configuration of [`createClient`](lib/src/client.dart) mirrors the upstream JS package — see the dartdoc on each parameter.

## Flutter web / custom transports

The default transport uses `dart:io.WebSocket`, which is unavailable on the web. Plug in `package:web_socket_channel` with a small adapter — see [`example/web_socket_channel_adapter.dart`](example/web_socket_channel_adapter.dart) for a copy-pasteable implementation:

```dart
final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  connector: webSocketChannelConnector,
);
```

`graphql_ws` itself has zero external dependencies — copy the adapter into your app, which already depends on `web_socket_channel`.

## Status

Pre-release (`0.1.0-dev`). The protocol layer and connection state machine are stable and covered by tests, but the API may shift before 1.0. Production deployments on iOS/Android are tracked toward an eventual 0.1.0 release.

The current `dart:io.WebSocket` adapter does not implement mobile-lifecycle aware behavior (foreground/background, network change re-handshake). Apps that need that should drive `client.terminate()` from their own lifecycle listeners. A first-class adapter is on the roadmap.

## Testing

```sh
dart pub get
dart test         # 131 cases, ~1s
dart analyze      # strict-casts, strict-inference, strict-raw-types
```

## License

MIT — see [LICENSE](LICENSE). Original work © Denis Badurina.
