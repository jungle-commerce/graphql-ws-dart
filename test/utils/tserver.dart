/// Scriptable WebSocket test server backed by `dart:io.HttpServer`.
///
/// Mirrors the role of `startTServer` from the JavaScript graphql-ws test
/// utilities, but at a lower level: tests drive incoming/outgoing frames
/// directly rather than going through a full GraphQL server. This keeps
/// the harness dependency-free and makes protocol-level behavior trivial
/// to assert.
library;

import 'dart:async';
import 'dart:io';

import 'package:graphql_ws/graphql_ws.dart';

import 'deferred.dart';

const _defaultTimeout = Duration(seconds: 1);

class TServer {
  TServer._(this._httpServer, this.uri);

  final HttpServer _httpServer;

  /// URI to pass to the client under test.
  final Uri uri;

  final List<TServerClient> _clients = [];
  final List<Deferred<TServerClient>> _pendingClientWaiters = [];
  bool _disposed = false;

  /// All connected clients in connection order. Closed/disconnected clients
  /// are filtered out — mirrors JS `getClients()` semantics.
  List<TServerClient> get clients =>
      List.unmodifiable(_clients.where((c) => c.isAlive));

  /// All clients ever seen, including disconnected ones (useful in tests
  /// asserting historical connection counts).
  List<TServerClient> get allClients => List.unmodifiable(_clients);

  /// Spin up a fresh server bound to an ephemeral loopback port.
  ///
  /// If [protocolSelector] is supplied, it is invoked with the requested
  /// sub-protocols and must return the selected protocol (or `null` to
  /// reject the upgrade with HTTP 400).
  static Future<TServer> start({
    String? Function(List<String> requested)? protocolSelector,
  }) async {
    final httpServer = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final uri = Uri.parse('ws://${httpServer.address.host}:${httpServer.port}');
    final server = TServer._(httpServer, uri);

    httpServer.listen((req) async {
      if (!WebSocketTransformer.isUpgradeRequest(req)) {
        req.response.statusCode = HttpStatus.badRequest;
        await req.response.close();
        return;
      }
      try {
        final socket = await WebSocketTransformer.upgrade(
          req,
          protocolSelector: protocolSelector ??
              (protocols) =>
                  protocols.contains(graphqlTransportWsProtocol)
                      ? graphqlTransportWsProtocol
                      : null,
        );
        final client = TServerClient._(socket);
        server._clients.add(client);
        if (server._pendingClientWaiters.isNotEmpty) {
          server._pendingClientWaiters.removeAt(0).resolve(client);
        }
      } on Object {
        // Upgrade rejected; client will see the failure on its side.
      }
    });

    return server;
  }

  /// Waits for the next (or already-arrived) client connection.
  Future<TServerClient> waitForClient({
    Duration timeout = _defaultTimeout,
  }) async {
    if (_clients.length > _consumedClients) {
      final c = _clients[_consumedClients++];
      return c;
    }
    final d = Deferred<TServerClient>();
    _pendingClientWaiters.add(d);
    return d.future.timeout(timeout, onTimeout: () {
      _pendingClientWaiters.remove(d);
      throw TimeoutException(
          'TServer: timed out waiting for client after ${timeout.inMilliseconds}ms');
    }).then((c) {
      _consumedClients++;
      return c;
    });
  }

  int _consumedClients = 0;

  /// Convenience: wait for a client, accept its `ConnectionInit`, and reply
  /// with `ConnectionAck` carrying the optional [ackPayload].
  Future<TServerClient> waitForConnectAndAck({
    Map<String, Object?>? ackPayload,
    Duration timeout = _defaultTimeout,
  }) async {
    final client = await waitForClient(timeout: timeout);
    await client.waitForMessageOfType(MessageType.connectionInit,
        timeout: timeout);
    client.sendMessage(ConnectionAckMessage(payload: ackPayload));
    return client;
  }

  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    for (final c in _clients) {
      await c._dispose();
    }
    await _httpServer.close(force: true);
  }
}

/// One server-side connection. Tests script the conversation by `send`ing
/// frames or messages and awaiting `waitFor…` helpers.
class TServerClient {
  TServerClient._(this._socket) {
    _socket.listen(
      (Object? frame) {
        if (frame is! String) return;
        _messages.add(frame);
        if (_pendingMessageWaiters.isNotEmpty) {
          _pendingMessageWaiters.removeAt(0).resolve(frame);
        }
      },
      onDone: () {
        if (!_closeCompleter.isCompleted) {
          _closeCompleter.complete(WebSocketCloseEvent(
            code: _socket.closeCode,
            reason: _socket.closeReason,
          ));
        }
      },
      onError: (Object err, StackTrace st) {
        if (!_closeCompleter.isCompleted) {
          _closeCompleter.completeError(err, st);
        }
      },
      cancelOnError: false,
    );
  }

  final WebSocket _socket;
  final List<String> _messages = [];
  int _consumedMessages = 0;
  final List<Deferred<String>> _pendingMessageWaiters = [];
  final Completer<WebSocketCloseEvent> _closeCompleter =
      Completer<WebSocketCloseEvent>();

  bool get isAlive => !_closeCompleter.isCompleted;

  /// All raw text frames received, in order.
  List<String> get receivedMessages => List.unmodifiable(_messages);

  /// Future completing when this client disconnects.
  Future<WebSocketCloseEvent> get done => _closeCompleter.future;

  void send(String data) => _socket.add(data);

  void sendMessage(Message message) =>
      _socket.add(stringifyMessage(message));

  Future<void> close([int? code, String? reason]) async {
    await _socket.close(code, reason);
  }

  Future<void> _dispose() async {
    try {
      await _socket.close();
    } on Object {
      // ignore
    }
  }

  /// Waits for the next raw text frame.
  Future<String> waitForRawMessage({Duration timeout = _defaultTimeout}) {
    if (_messages.length > _consumedMessages) {
      return Future.value(_messages[_consumedMessages++]);
    }
    final d = Deferred<String>();
    _pendingMessageWaiters.add(d);
    return d.future.timeout(timeout, onTimeout: () {
      _pendingMessageWaiters.remove(d);
      throw TimeoutException(
          'TServerClient: timed out waiting for message after ${timeout.inMilliseconds}ms');
    }).then((m) {
      _consumedMessages++;
      return m;
    });
  }

  /// Waits for the next message and validates its type.
  Future<Message> waitForMessage({Duration timeout = _defaultTimeout}) async {
    final raw = await waitForRawMessage(timeout: timeout);
    return parseMessage(raw);
  }

  /// Waits for the next message of the given [type] (skipping nothing — the
  /// next one must be of that type or this throws).
  Future<Message> waitForMessageOfType(
    MessageType type, {
    Duration timeout = _defaultTimeout,
  }) async {
    final m = await waitForMessage(timeout: timeout);
    if (m.type != type) {
      throw StateError(
          'Expected ${type.wireValue} message, got ${m.type.wireValue}: $m');
    }
    return m;
  }

  /// Convenience: wait for the next `Subscribe` frame and return it typed.
  Future<SubscribeMessage> waitForSubscribe({
    Duration timeout = _defaultTimeout,
  }) async {
    final m = await waitForMessageOfType(MessageType.subscribe,
        timeout: timeout);
    return m as SubscribeMessage;
  }

  /// Waits until the client disconnects (with optional timeout).
  Future<WebSocketCloseEvent> waitForClose({
    Duration timeout = _defaultTimeout,
  }) {
    return done.timeout(timeout, onTimeout: () {
      throw TimeoutException(
          'TServerClient: timed out waiting for close after ${timeout.inMilliseconds}ms');
    });
  }
}
