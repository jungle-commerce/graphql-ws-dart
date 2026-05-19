/// Drop-in `WebSocketAdapter` backed by `package:web_socket_channel`.
///
/// Use this on Flutter web (where `dart:io.WebSocket` is unavailable) or
/// anywhere you already depend on `web_socket_channel`. Copy this file
/// into your app — `graphql_ws` itself has zero external dependencies, so
/// it does not ship a binding to `web_socket_channel`.
///
/// Add the dependency to your app's pubspec:
///
/// ```yaml
/// dependencies:
///   graphql_ws: ^0.1.0
///   web_socket_channel: ^3.0.0
/// ```
///
/// Then pass [webSocketChannelConnector] as the client's `connector`:
///
/// ```dart
/// final client = createClient(
///   url: () => Uri.parse('wss://example.com/graphql'),
///   connector: webSocketChannelConnector,
/// );
/// ```
library;

import 'dart:async';

import 'package:graphql_ws/graphql_ws.dart';
// `web_socket_channel` is intentionally NOT a dependency of `graphql_ws` —
// see the doc-comment above. Copy this file into your own package which
// does depend on it.
// ignore: depend_on_referenced_packages
import 'package:web_socket_channel/web_socket_channel.dart';

/// A [WebSocketConnector] backed by `package:web_socket_channel`. Works on
/// VM, web, and Flutter targets.
Future<WebSocketAdapter> webSocketChannelConnector(
  Uri uri,
  Iterable<String> protocols,
) async {
  final channel = WebSocketChannel.connect(uri, protocols: protocols);
  await channel.ready;
  return _ChannelAdapter(channel);
}

class _ChannelAdapter implements WebSocketAdapter {
  _ChannelAdapter(this._channel) {
    _messages = _channel.stream
        .where((Object? frame) => frame is String)
        .cast<String>();
    _channel.sink.done.then((Object? _) {
      if (_doneCompleter.isCompleted) return;
      _doneCompleter.complete(WebSocketCloseEvent(
        code: _channel.closeCode ?? _localCloseCode,
        reason: _channel.closeReason ?? _localCloseReason,
        wasClean: _wasClean,
      ));
    }, onError: (Object err, StackTrace st) {
      if (_doneCompleter.isCompleted) return;
      _doneCompleter.completeError(err, st);
    });
  }

  final WebSocketChannel _channel;
  late final Stream<String> _messages;
  final Completer<WebSocketCloseEvent> _doneCompleter =
      Completer<WebSocketCloseEvent>();
  bool _wasClean = true;
  int? _localCloseCode;
  String? _localCloseReason;

  @override
  Stream<String> get messages => _messages;

  @override
  Future<WebSocketCloseEvent> get done => _doneCompleter.future;

  @override
  bool get isOpen => _channel.closeCode == null && !_doneCompleter.isCompleted;

  @override
  void sendText(String data) {
    if (!isOpen) return;
    _channel.sink.add(data);
  }

  @override
  void close([int? code, String? reason]) {
    _localCloseCode = code;
    _localCloseReason = reason;
    _wasClean = code == 1000 || code == null;
    _channel.sink.close(code, reason);
  }
}
