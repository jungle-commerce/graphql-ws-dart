/// Resolves the platform-appropriate default WebSocket connector.
///
/// On native targets (`dart.library.io` available) this re-exports the
/// `dart:io`-backed connector and `DartIoWebSocketAdapter`; everywhere else
/// (the web) it re-exports a throwing stub. Either way the consuming code
/// sees a `defaultWebSocketConnector` of the same shape — `dart:io` is never
/// imported into a web compile.
library;

export 'default_connector_stub.dart'
    if (dart.library.io) 'default_connector_io.dart';
