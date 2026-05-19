# graphql-ws-dart

A monorepo housing a Dart port of [`graphql-ws`](https://github.com/enisdenjo/graphql-ws) and its companion packages. Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md) (`graphql-transport-ws` sub-protocol).

## Packages

| Package | Path | Description |
|---|---|---|
| [`graphql_ws`](packages/graphql_ws) | [`packages/graphql_ws`](packages/graphql_ws) | Zero-dependency, lazy, simple GraphQL over WebSocket client. Default transport via `dart:io.WebSocket`. |
| [`graphql_ws_web_socket_channel_connector`](packages/graphql_ws_web_socket_channel_connector) | [`packages/graphql_ws_web_socket_channel_connector`](packages/graphql_ws_web_socket_channel_connector) | Drop-in `WebSocketConnector` backed by `package:web_socket_channel`. Use this on Flutter web. |

## Working in this repo

This is a [pub workspace](https://dart.dev/tools/pub/workspaces) (requires Dart 3.5+). One `dart pub get` at the root resolves every package.

```sh
dart pub get               # resolve deps across the workspace
dart analyze               # analyze every package
dart test                  # run every package's tests
```

To work on a single package, `cd` into its directory:

```sh
cd packages/graphql_ws
dart test test/client_test.dart
```

## License

MIT — see [LICENSE](LICENSE).
