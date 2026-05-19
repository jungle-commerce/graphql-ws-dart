## 0.1.0-dev

- Initial Dart port of [graphql-ws](https://github.com/enisdenjo/graphql-ws) (client only).
- Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md).
- Zero external dependencies. Default transport uses `dart:io.WebSocket`; users can supply a custom `WebSocketAdapter` for other platforms (e.g. Flutter web).
