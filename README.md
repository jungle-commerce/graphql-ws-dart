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

### Flutter version

The repo pins Flutter via [FVM](https://fvm.app/). Install FVM and run `fvm install` once; afterwards prefix Flutter/Dart commands with `fvm`:

```sh
fvm install                # install the pinned Flutter version (one-time)
fvm flutter pub get        # resolve deps across the workspace
fvm dart analyze           # analyze every package
```

Tests run per-package — pure-Dart packages with `fvm dart test`, the Flutter package with `fvm flutter test`:

```sh
fvm dart test packages/graphql_ws
fvm flutter test packages/graphql_ws_flutter
```

## Credits

This library is a Dart port of [graphql-ws](https://github.com/enisdenjo/graphql-ws) by [Denis Badurina](https://github.com/enisdenjo). The protocol semantics, client state machine, retry logic, and test structure are all derived from his work. Thank you Denis. 🙏

## License

MIT — see [LICENSE](LICENSE).
