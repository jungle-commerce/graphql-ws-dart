## 0.1.1

- `isOpen` is `false` as soon as the adapter starts a close. The channel's `closeCode` is set only once the peer answers, and the sink throws on `add` before that, so cancelling a subscription right after `dispose()`, `terminate()` or a lazy close completed `StreamSubscription.cancel()` with an error.

## 0.1.0

- Initial release. Exposes `webSocketChannelConnector` — a `WebSocketConnector`
  for [`graphql_ws`](https://pub.dev/packages/graphql_ws) backed by
  [`package:web_socket_channel`](https://pub.dev/packages/web_socket_channel).
  Works on the Dart VM, web, and Flutter.
