# graphql_ws

Coherent, zero-dependency, lazy, simple GraphQL over WebSocket Protocol compliant **client** for Dart and Flutter.

A faithful port of the JavaScript [`graphql-ws`](https://github.com/enisdenjo/graphql-ws) client. Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md) (`graphql-transport-ws` sub-protocol).

- **Zero external dependencies.**
- **131 unit + 17 integration tests** covering protocol validation, lifecycle, retries, lazy/keepalive, ping/pong, terminate, streaming, plus end-to-end against a real `shelf`-backed graphql-transport-ws server.
- Native transport via `dart:io.WebSocket`; Flutter web + custom transports via a small adapter.
- **Strict typing throughout** — `Object?`, no `dynamic`; analyzer enforces `strict-casts`, `strict-inference`, `strict-raw-types`.

## Table of contents

- [Install](#install)
- [Quick start](#quick-start)
- [Subscriptions: `stream()` vs `subscribe()`](#subscriptions-stream-vs-subscribe)
- [Lifecycle events](#lifecycle-events)
- [Authentication](#authentication)
- [Recipes](#recipes)
  - [Keep-alive watchdog](#keep-alive-watchdog)
  - [Custom retry backoff](#custom-retry-backoff)
  - [Refreshing the URL per attempt](#refreshing-the-url-per-attempt)
- [Error handling](#error-handling)
- [Flutter web / custom transports](#flutter-web--custom-transports)
- [`createClient` options](#createclient-options)
- [Status & roadmap](#status--roadmap)
- [Testing](#testing)
- [License](#license)

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

## Subscriptions: `stream()` vs `subscribe()`

Two equivalent APIs are exposed. Pick whichever fits your code.

**`stream<TData, TExtensions>(payload)` — recommended for Dart.** Returns a regular `Stream<FormattedExecutionResult<TData, TExtensions>>`. Cancel the subscription via `subscription.cancel()`. Integrates with `await for`, `StreamBuilder`, `StreamSubscription`, etc.

```dart
final results = client.stream<Map<String, Object?>, Map<String, Object?>>(
  const SubscribePayload(query: 'subscription { tick }'),
);
await for (final r in results) {
  print(r.data);
}
```

**`subscribe(payload, sink)` — 1:1 with the JS client.** Takes a `Sink<T>` (the package's own three-method interface: `next` / `error` / `complete`). Returns a dispose callback.

```dart
final dispose = client.subscribe<Map<String, Object?>, Map<String, Object?>>(
  const SubscribePayload(query: 'subscription { tick }'),
  _MySink(),
);
// later:
dispose();
```

Internally `stream()` is implemented on top of `subscribe()` — there is no performance difference.

`SubscribePayload` accepts `query`, `operationName`, `variables`, and `extensions`. Null/omitted fields are serialised as absent (not `null`) on the wire.

## Lifecycle events

`ClientEvent` is a sealed class hierarchy. Subscribe with `Client.on<E>(listener)`:

```dart
final off = client.on<ClientEvent>((event) {
  switch (event) {
    case ConnectingEvent(:final isRetry):
      print('connecting (retry=$isRetry)');
    case ConnectedEvent(:final wasRetry, :final payload):
      print('connected (recovered=$wasRetry, ack=$payload)');
    case MessageEvent(:final message):
      // every valid inbound frame
    case PingEvent() || PongEvent():
      // keep-alive observability
    case ClosedEvent(:final code, :final reason):
      print('closed $code $reason');
    case ErrorEvent(:final error):
      print('internal error: $error');
    case OpenedEvent():
      // socket open but not yet acknowledged
  }
});

// Narrow the listener to one event:
client.on<ConnectedEvent>((e) => print(e.wasRetry));

// Pass initial listeners at construction:
createClient(
  url: () => uri,
  on: {
    ClosedEvent: (e) {
      if (e is ClosedEvent) report(e);
    },
  },
);
```

The full event list: `ConnectingEvent`, `OpenedEvent`, `ConnectedEvent`, `MessageEvent`, `PingEvent`, `PongEvent`, `ClosedEvent`, `ErrorEvent`.

## Authentication

Pass auth credentials in `connectionParams` — the value is sent inside the `ConnectionInit` payload, which most servers gate on.

```dart
createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  connectionParams: () async {
    final token = await fetchFreshToken();
    return {'authToken': token};
  },
);
```

`connectionParams` is called *every* connection attempt — including retries — so it's the right hook for refreshed-on-reconnect tokens.

If `connectionParams` throws (sync or async), the socket is closed with code `4005 InternalClientError` and the thrown value is surfaced to subscription sinks.

## Recipes

### Keep-alive watchdog

`graphql_ws` will *send* pings at the `keepAlive` interval but, per the JS client, will NOT automatically tear down a connection if the server stops responding. Layer your own watchdog on top:

```dart
Timer? timedOut;

final client = createClient(
  url: () => uri,
  keepAlive: const Duration(seconds: 10),
);

client.on<PingEvent>((e) {
  if (e.received) return; // server-initiated, ignore
  // Client just sent a ping — give the server N seconds to respond.
  timedOut = Timer(const Duration(seconds: 5), () {
    client.terminate(); // synthetic 4499 close, triggers retry
  });
});

client.on<PongEvent>((e) {
  if (e.received) timedOut?.cancel();
});
```

### Custom retry backoff

```dart
createClient(
  url: () => uri,
  retryAttempts: 10,
  retryWait: (retries) async {
    final ms = (100 * (1 << retries)).clamp(0, 30000);
    await Future<void>.delayed(Duration(milliseconds: ms));
  },
);
```

To control which errors are retryable:

```dart
shouldRetry: (errOrCloseEvent) {
  if (errOrCloseEvent is LikeCloseEvent && errOrCloseEvent.code == 4403) {
    return false; // give up on forbidden
  }
  return true;
},
```

Note: some close codes (`4005`, `4400`, `4401`, `4406`, `4409`, `4429`, `4500`) are classified as **fatal** by the library and short-circuit `shouldRetry` — see the dartdoc on `createClient`'s `retryAttempts`.

### Refreshing the URL per attempt

```dart
createClient(
  url: () async {
    final token = await refreshAccessToken();
    return Uri.parse('wss://example.com/graphql?token=$token');
  },
);
```

The `url` callback is invoked on every connection attempt — useful for query-string-based auth that requires a fresh token after the previous connection died.

## Error handling

The error types your sinks / streams will see:

| Cause | Type delivered to `sink.error` / `onError` |
|---|---|
| Server closed with a code | `LikeCloseEvent { code, reason }` |
| Server sent an `Error` message | `List<GraphQLFormattedError>` |
| Transport / DNS / refused | The underlying exception (e.g. `SocketException`) |
| Client-side bad-response classification | `LikeCloseEvent(code: 4004)` |
| `connectionParams` threw | The user's thrown value (then `LikeCloseEvent(4005)`) |
| Stuck socket via `terminate()` | `LikeCloseEvent(code: 4499, reason: 'Terminated')` |

The `LikeCloseEvent` type is intentionally minimal — it does NOT extend `Exception` because it represents a *clean* protocol-level disconnect (not an error per se). Use `is LikeCloseEvent` to discriminate.

## Flutter web / custom transports

The default transport uses `dart:io.WebSocket`, which is unavailable on the web. For a drop-in `web_socket_channel`-backed connector, add the companion package:

```yaml
dependencies:
  graphql_ws: ^0.1.0
  graphql_ws_web_socket_channel_connector: ^0.1.0
```

```dart
import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_web_socket_channel_connector/graphql_ws_web_socket_channel_connector.dart';

final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  connector: webSocketChannelConnector,
);
```

`graphql_ws` itself stays zero-dependency — the connector is an opt-in companion package. The same pattern works for any transport: implement `WebSocketAdapter`, expose a `WebSocketConnector` factory, pass it as `connector:`.

## `createClient` options

| Option | Type | Default | Purpose |
|---|---|---|---|
| `url` | `FutureOr<Uri> Function()` | required | Resolved on every connect attempt. |
| `connectionParams` | `FutureOr<Map<String, Object?>?> Function()?` | `null` | Payload sent with `ConnectionInit`. |
| `lazy` | `bool` | `true` | Connect on first subscribe; close after last unsubscribe. |
| `onNonLazyError` | `void Function(Object)?` | — | **Required** when `lazy: false`. Surfaces unrecoverable errors. |
| `lazyCloseTimeout` | `Duration` | `Duration.zero` | Debounce close after the last subscriber leaves. |
| `keepAlive` | `Duration` | `Duration.zero` | Interval between client-initiated `Ping`s. |
| `connectionAckWaitTimeout` | `Duration` | `Duration.zero` | Fail if `ConnectionAck` doesn't arrive within this window. |
| `disablePong` | `bool` | `false` | Skip the automatic `Pong` reply to server pings. |
| `retryAttempts` | `int` | `5` | Reconnect budget for non-fatal disconnects. |
| `retryWait` | `Future<void> Function(int)?` | randomised exponential backoff | Delay between attempts. |
| `shouldRetry` | `bool Function(Object)?` | only retry close-event-likes | Custom retry classifier. |
| `generateID` | `String Function(SubscribePayload)?` | UUID v4 | Subscription id source. |
| `jsonMessageReviver` | `Object? Function(Object?, Object?)?` | — | Passed to `jsonDecode`. |
| `connector` | `WebSocketConnector?` | `dart:io.WebSocket` | Transport injection. |
| `on` | `Map<Type, ClientEventListener<ClientEvent>>?` | — | Initial event listeners. |

See the dartdoc on `createClient` for the full spec on each parameter.

## Status & roadmap

Pre-release (`0.1.0-dev`). The protocol layer and connection state machine are stable and covered by tests, but the API may shift before 1.0.

Not yet shipped, on the roadmap:

- **Mobile-lifecycle aware adapter** — automatic `terminate()` on app backgrounding, network-change-aware reconnects, iOS/Android-specific socket-stuck heuristics. Apps that need this today should drive `client.terminate()` from their own `WidgetsBindingObserver` / `Connectivity` listeners.

## Testing

```sh
dart pub get
dart analyze              # strict-casts, strict-inference, strict-raw-types
dart test                 # full suite (~1s)
dart test test/client_test.dart
dart test -N 'lazy'       # tests whose names contain "lazy"
dart test --tags client   # see dart_test.yaml for tag groups
```

Two test layers:

- **Unit suite** (`test/{common,client,smoke}_test.dart`) — runs against [`test/utils/tserver.dart`](test/utils/tserver.dart), a scriptable `dart:io.HttpServer`-backed mock that drives `ConnectionAck`/`Next`/`Error`/`Complete` frames directly. Fast, dependency-free.
- **Integration suite** (`test/integration_test.dart`, tagged `integration`) — runs against the in-repo `graphql_ws_test_server` package, a real `shelf` + `shelf_web_socket` graphql-transport-ws server. Exercises the default `DartIoWebSocketAdapter` end-to-end.

## License

MIT — see [LICENSE](../../LICENSE). Based on the JavaScript [`graphql-ws`](https://github.com/enisdenjo/graphql-ws) client by Denis Badurina.
