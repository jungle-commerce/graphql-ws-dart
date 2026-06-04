## 0.1.1

- Treat close code 1002 (Protocol Error) as non-fatal/retryable. Android emits 1002 on abrupt network disconnects, not only true protocol violations, so classifying it as fatal tore down subscriptions on every network drop.
- Normalise a transport failure to *establish* the connection (e.g. `dart:io` throwing `SocketException` on a failed DNS lookup while offline) to an abnormal closure (1006) instead of propagating the raw error. A raw exception bypassed `shouldRetry` and killed the subscription; 1006 is non-fatal, so the retry machinery (and `connectivityAwareRetryWait`'s offline parking) now runs as it does for a mid-session drop. This closes a Dart-vs-JS gap: JS surfaces connection failures as close events, but `dart:io` throws.

## 0.1.0

- Initial Dart port of [graphql-ws](https://github.com/enisdenjo/graphql-ws) (client only).
- Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md).
- Zero external dependencies. Default transport uses `dart:io.WebSocket`; users can supply a custom `WebSocketAdapter` for other platforms (e.g. Flutter web).
- Add `Client.ping([payload])` — sends a `Ping` on demand (a no-op without a live connection). Useful as a liveness probe; the matching `Pong` surfaces as a `PongEvent`. No JS counterpart.
- `dart:io` is no longer imported into a web compile. The default connector is now resolved via a conditional export (`default_connector.dart`): the `dart:io` connector on native, a throwing stub on the web. `defaultWebSocketConnector` on the web now throws a clear `UnsupportedError` pointing at `graphql_ws_web_socket_channel_connector`, instead of failing murkily. `DartIoWebSocketAdapter` remains exported on native targets.
