# graphql_ws_web_socket_channel_connector

A [`WebSocketConnector`](https://pub.dev/documentation/graphql_ws/latest/graphql_ws/WebSocketConnector.html)
for [`graphql_ws`](https://pub.dev/packages/graphql_ws) backed by
[`package:web_socket_channel`](https://pub.dev/packages/web_socket_channel).

`graphql_ws` itself has zero external dependencies and ships a `dart:io.WebSocket`
transport by default. That works on the VM and on Flutter mobile/desktop, but
not on the web. This package bridges `web_socket_channel` — which works on VM,
web, and Flutter — so the same client code runs everywhere.

## Install

```yaml
dependencies:
  graphql_ws: ^0.1.0
  graphql_ws_web_socket_channel_connector: ^0.1.0
```

## Usage

```dart
import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_web_socket_channel_connector/graphql_ws_web_socket_channel_connector.dart';

final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  connector: webSocketChannelConnector,
);
```

That's the whole API surface — one top-level function you pass as `connector`.

## License

MIT — see [LICENSE](../../LICENSE).
