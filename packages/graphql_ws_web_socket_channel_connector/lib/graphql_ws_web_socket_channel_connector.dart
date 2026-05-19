/// A `WebSocketConnector` for [`graphql_ws`][graphql_ws] backed by
/// [`package:web_socket_channel`][web_socket_channel].
///
/// `graphql_ws` itself has zero external dependencies and ships a
/// `dart:io.WebSocket` transport by default. That works on the VM and on
/// Flutter mobile/desktop, but not on the web. This package bridges
/// `package:web_socket_channel` — which works on VM, web, and Flutter — so
/// the same client code runs everywhere.
///
/// ```dart
/// import 'package:graphql_ws/graphql_ws.dart';
/// import 'package:graphql_ws_web_socket_channel_connector/graphql_ws_web_socket_channel_connector.dart';
///
/// final client = createClient(
///   url: () => Uri.parse('wss://example.com/graphql'),
///   connector: webSocketChannelConnector,
/// );
/// ```
///
/// [graphql_ws]: https://pub.dev/packages/graphql_ws
/// [web_socket_channel]: https://pub.dev/packages/web_socket_channel
library;

import 'dart:async';

import 'package:graphql_ws/graphql_ws.dart';
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
