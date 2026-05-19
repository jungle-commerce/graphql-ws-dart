/// Pluggable WebSocket transport abstraction used by the client.
///
/// The library ships a [defaultWebSocketConnector] backed by `dart:io.WebSocket`
/// for native (server, iOS, Android, desktop) targets. For Flutter web,
/// supply a custom [WebSocketConnector] that wraps `package:web_socket_channel`
/// or any other browser-compatible WebSocket implementation.
library;

import 'dart:async';
import 'dart:io' as io;

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

/// Default [WebSocketConnector] implementation backed by `dart:io.WebSocket`.
///
/// Works on all native targets (server, mobile, desktop). Does **not** work
/// in Flutter web — provide a custom connector for web targets.
Future<WebSocketAdapter> defaultWebSocketConnector(
  Uri uri,
  Iterable<String> protocols,
) async {
  final socket = await io.WebSocket.connect(
    uri.toString(),
    protocols: protocols,
  );
  return DartIoWebSocketAdapter(socket);
}

/// [WebSocketAdapter] implementation backed by `dart:io.WebSocket`.
///
/// Public so that users implementing their own connector can wrap an
/// existing `io.WebSocket` instance without rolling their own adapter.
class DartIoWebSocketAdapter implements WebSocketAdapter {
  DartIoWebSocketAdapter(this._socket) {
    _doneCompleter = Completer<WebSocketCloseEvent>();
    // `io.WebSocket` is a `Stream<dynamic>` carrying either `String` or
    // `List<int>` frames. We coerce to text-only and propagate close on the
    // done future.
    _messages = _socket
        .where((Object? frame) => frame is String)
        .cast<String>();
    _socket.done.then((Object? _) {
      if (_doneCompleter.isCompleted) return;
      // `dart:io` exposes `closeCode`/`closeReason` reflecting the
      // *remote* side's close frame. When the close is initiated locally
      // and the remote did not echo a custom code, those values fall back
      // to 1005 (No Status Received). In that case, prefer the code we
      // actually requested so consumers see "the close I asked for".
      final code = _socket.closeCode ?? _localCloseCode;
      final reason = (_socket.closeReason != null &&
              _socket.closeReason!.isNotEmpty)
          ? _socket.closeReason
          : _localCloseReason ?? _socket.closeReason;
      _doneCompleter.complete(
        WebSocketCloseEvent(
          code: _localCloseInitiated && _localCloseCode != null
              ? _localCloseCode
              : code,
          reason: _localCloseInitiated && _localCloseReason != null
              ? _localCloseReason
              : reason,
          wasClean: _wasClean,
        ),
      );
    }, onError: (Object err, StackTrace st) {
      if (_doneCompleter.isCompleted) return;
      _doneCompleter.completeError(err, st);
    });
  }

  final io.WebSocket _socket;
  late final Stream<String> _messages;
  late final Completer<WebSocketCloseEvent> _doneCompleter;
  bool _wasClean = true;
  bool _localCloseInitiated = false;
  int? _localCloseCode;
  String? _localCloseReason;

  @override
  Stream<String> get messages => _messages;

  @override
  Future<WebSocketCloseEvent> get done => _doneCompleter.future;

  @override
  bool get isOpen => _socket.readyState == io.WebSocket.open;

  @override
  void sendText(String data) {
    if (!isOpen) return;
    _socket.add(data);
  }

  @override
  void close([int? code, String? reason]) {
    _localCloseInitiated = true;
    _localCloseCode = code;
    _localCloseReason = reason;
    _wasClean = code == 1000 || code == null;
    // `dart:io` rejects close codes outside 1000-4999 with an ArgumentError;
    // surface as a normal close so the client's error pipeline isn't blown
    // up by transports that disagree on validation.
    try {
      _socket.close(code, reason);
    } on ArgumentError {
      _socket.close();
    }
  }
}
