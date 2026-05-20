# graphql-ws-dart

A monorepo housing a Dart port of [`graphql-ws`](https://github.com/enisdenjo/graphql-ws) and its companion packages. Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md) (`graphql-transport-ws` sub-protocol).

## Packages

| Package | Path | Description |
|---|---|---|
| [`graphql_ws`](packages/graphql_ws) | [`packages/graphql_ws`](packages/graphql_ws) | Zero-dependency, lazy, simple GraphQL over WebSocket client. Default transport via `dart:io.WebSocket`. |
| [`graphql_ws_web_socket_channel_connector`](packages/graphql_ws_web_socket_channel_connector) | [`packages/graphql_ws_web_socket_channel_connector`](packages/graphql_ws_web_socket_channel_connector) | Drop-in `WebSocketConnector` backed by `package:web_socket_channel`. Use this on Flutter web. |
| [`graphql_ws_flutter`](packages/graphql_ws_flutter) | [`packages/graphql_ws_flutter`](packages/graphql_ws_flutter) | Flutter integration — keeps the connection alive across app lifecycle changes and flaky mobile connectivity. |

## Working in this repo

This is a [pub workspace](https://dart.dev/tools/pub/workspaces) (requires Dart 3.5+). One resolve at the root covers every package.

Because `graphql_ws_flutter` depends on the Flutter SDK, the workspace is resolved with `flutter pub get` (not `dart pub get`):

```sh
flutter pub get            # resolve deps across the workspace
dart analyze               # analyze every package
```

Tests run per-package — pure-Dart packages with `dart test`, the Flutter package with `flutter test`:

```sh
dart test packages/graphql_ws
flutter test packages/graphql_ws_flutter
```

## License

MIT — see [LICENSE](LICENSE).
