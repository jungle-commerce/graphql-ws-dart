/// `dart:io`-backed default WebSocket transport.
///
/// Selected by `default_connector.dart` on native targets (server, mobile,
/// desktop). Never compiled for the web — see `default_connector_stub.dart`.
library;

import 'dart:async';
import 'dart:io' as io;

import 'websocket_adapter.dart';

/// Default [WebSocketConnector] implementation backed by `dart:io.WebSocket`.
///
/// Works on all native targets (server, mobile, desktop).
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
/// Public so that users implementing their own native connector can wrap an
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
