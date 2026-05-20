/// Pluggable WebSocket transport abstraction used by the client.
///
/// This file is platform-neutral — it imports no `dart:io` or `dart:html`.
/// The concrete default transport ([defaultWebSocketConnector]) lives in
/// `default_connector.dart`, which conditionally pulls in a `dart:io`-backed
/// implementation on native targets and a throwing stub on the web.
///
/// For Flutter web, supply a custom [WebSocketConnector] — e.g.
/// `webSocketChannelConnector` from `package:graphql_ws_web_socket_channel_connector`.
library;

import 'dart:async';

/// Creates and returns an open [WebSocketAdapter].
///
/// Called by the client for every connection attempt. The returned adapter
/// must already have completed its handshake (i.e. be ready to receive
/// frames). The connector is permitted to throw — the client will treat
/// throwing as a connection failure and apply its retry policy.
typedef WebSocketConnector = Future<WebSocketAdapter> Function(
  Uri uri,
  Iterable<String> protocols,
);

/// Transport adapter abstraction for a single WebSocket connection.
///
/// Implementations wrap a concrete WebSocket (`dart:io`, `web_socket_channel`,
/// a mock for tests, …) and expose just the operations the client needs.
///
/// **Lifecycle**: the adapter is constructed already-open. Once [close] is
/// called (locally) or the remote endpoint closes the connection, [done]
/// completes and no further frames may be sent or received.
abstract interface class WebSocketAdapter {
  /// Stream of incoming text frames.
  ///
  /// Must be a single-subscription stream. The client subscribes once for the
  /// lifetime of the adapter and unlistens before closing.
  Stream<String> get messages;

  /// Future completing when the connection closes (either locally or
  /// remotely). Resolves with the close code/reason as reported by the peer
  /// (or `null` if the underlying transport did not provide them).
  Future<WebSocketCloseEvent> get done;

  /// Send a text frame containing [data].
  ///
  /// Calling this on a closed adapter is a no-op (or implementation-defined).
  void sendText(String data);

  /// Close the connection. Both [code] and [reason] are optional per the
  /// WebSocket spec. After this call [done] is expected to complete.
  void close([int? code, String? reason]);

  /// The local view of whether the adapter is still open.
  bool get isOpen;
}

/// Describes a WebSocket close event, used as the resolution value of
/// [WebSocketAdapter.done].
class WebSocketCloseEvent {
  const WebSocketCloseEvent({this.code, this.reason, this.wasClean = true});

  /// The close code reported by the peer, or `null` if unavailable.
  final int? code;

  /// The close reason reported by the peer, or `null` if unavailable.
  final String? reason;

  /// Whether the connection terminated cleanly (graceful close handshake).
  final bool wasClean;

  @override
  String toString() =>
      'WebSocketCloseEvent(code: $code, reason: $reason, wasClean: $wasClean)';
}
