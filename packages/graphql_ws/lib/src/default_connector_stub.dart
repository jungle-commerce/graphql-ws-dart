/// Fallback default WebSocket transport for targets without `dart:io`
/// (notably the web).
///
/// Selected by `default_connector.dart` when `dart.library.io` is
/// unavailable. There is no built-in browser WebSocket transport — keeping a
/// browser socket implementation out of the core preserves its
/// zero-dependency promise — so this connector throws, pointing the caller
/// at the companion package.
library;

import 'websocket_adapter.dart';

/// Default [WebSocketConnector] on platforms without `dart:io`.
///
/// Always throws: the core library ships no web WebSocket transport. On the
/// web, pass an explicit `connector` to `createClient` — e.g.
/// `webSocketChannelConnector` from
/// `package:graphql_ws_web_socket_channel_connector`.
Future<WebSocketAdapter> defaultWebSocketConnector(
  Uri uri,
  Iterable<String> protocols,
) {
  throw UnsupportedError(
    'graphql_ws has no built-in WebSocket transport for this platform '
    '(dart:io is unavailable). Pass a `connector` to createClient — e.g. '
    'webSocketChannelConnector from package:graphql_ws_web_socket_channel_connector.',
  );
}
