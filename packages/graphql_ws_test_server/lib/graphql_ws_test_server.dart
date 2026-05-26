/// A minimal `graphql-transport-ws` compliant WebSocket server for
/// integration tests in this repo.
///
/// Not a real GraphQL executor — it dispatches incoming `subscribe` messages
/// to operation handlers registered by name (`operationName` in the payload).
/// Each handler returns a `Stream<Map<String, Object?>>` whose elements are
/// emitted as `next` messages, followed by a `complete` when the stream
/// closes. Stream errors are emitted as `error` messages.
///
/// Deliberately implements the protocol from scratch (no dependency on
/// `graphql_ws`) so that integration tests can't pass just because client
/// and server share buggy constants.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_web_socket/shelf_web_socket.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Handler invoked when a `subscribe` message arrives. Receives the raw
/// `payload` map (per protocol: `{query, operationName?, variables?, extensions?}`).
///
/// Yielded maps are sent verbatim as the `payload` of `next` messages — so
/// `{'data': {...}, 'errors': [...]}` is the typical shape, mirroring
/// GraphQL's `FormattedExecutionResult`. Stream errors become `error`
/// messages; close the stream (return) to emit `complete`.
typedef OperationHandler = Stream<Map<String, Object?>> Function(
    Map<String, Object?> payload);

/// Invoked when a `connection_init` frame arrives. Receives the payload
/// (the `payload` field of the message, which may be `null`).
///
/// Return `null` to accept the connection (server responds with
/// `connection_ack`). Return a close code (e.g. `4403`) to reject before
/// acking — the server will close with that code instead.
///
/// May be async — useful for simulating a slow ack to exercise the client's
/// `connectionAckWaitTimeout`.
typedef ConnectionInitHandler = FutureOr<int?> Function(
    Map<String, Object?>? payload);

/// A running test server. Use [start] to construct; call [dispose] to stop.
class GraphqlWsTestServer {
  GraphqlWsTestServer._();

  /// Starts an HTTP server on [host]:[port] that upgrades any WebSocket
  /// request to a `graphql-transport-ws` session.
  ///
  /// [host] defaults to the `SERVER_HOST` env var, then `'localhost'`. Use
  /// `'0.0.0.0'` to expose the server on the network (relevant when an
  /// emulator or remote device must reach the host). [port] defaults to 0,
  /// which picks a random free port; read [uri] after start to discover it.
  static Future<GraphqlWsTestServer> start({
    String? host,
    int port = 0,
  }) async {
    final effectiveHost =
        host ?? Platform.environment['SERVER_HOST'] ?? 'localhost';

    final server = GraphqlWsTestServer._();

    void onConnection(WebSocketChannel channel, String? subprotocol) {
      final session = _Session(channel, server);
      server._sessions.add(session);
      session.onClose = () => server._sessions.remove(session);
      session.start();
    }

    final handler = webSocketHandler(
      onConnection,
      protocols: const ['graphql-transport-ws'],
    );
    final httpServer = await shelf_io.serve(handler, effectiveHost, port);
    server._httpServer = httpServer;
    return server;
  }

  late HttpServer _httpServer;
  final Map<String, OperationHandler> _operations = {};
  final Set<_Session> _sessions = {};

  /// Optional hook invoked for every `connection_init` frame. Lets tests
  /// inspect the payload (e.g. assert auth tokens) or reject the connection
  /// by returning a close code instead of `null`.
  ConnectionInitHandler? onConnectionInit;

  /// Invoked when a `subscribe` message arrives, before the handler runs.
  /// Receives the subscription id and the raw payload. Useful for asserting
  /// that the client sent a specific id (e.g. from a custom [generateId]).
  void Function(String id, Map<String, Object?> payload)? onSubscribe;

  /// Number of `pong` messages received from clients since the server started.
  /// Reset to 0 per test by starting a fresh server in setUp.
  int get receivedPongCount => _receivedPongCount;
  int _receivedPongCount = 0;

  /// Whether the server replies to client `ping` frames with a `pong`.
  /// Set to `false` to simulate a frozen ("zombie") socket — the connection
  /// stays open but liveness probes go unanswered.
  bool respondToPings = true;

  /// Sends a `ping` frame to every connected client. Used to verify that the
  /// client responds with a `pong` (or, with [disablePong: true], stays silent).
  void pingAllClients({Map<String, Object?>? payload}) {
    for (final s in _sessions.toList()) {
      s.sendPing(payload: payload);
    }
  }

  /// The `ws://host:port` base URL. Path is unused (any path works).
  Uri get uri => Uri(
        scheme: 'ws',
        host: _httpServer.address.host,
        port: _httpServer.port,
      );

  /// Registers a handler for `subscribe` messages whose payload's
  /// `operationName` equals [name]. Overwrites any previous handler.
  void register(String name, OperationHandler handler) {
    _operations[name] = handler;
  }

  /// Number of currently-connected clients. Useful in tests that assert
  /// the client tore the socket down cleanly.
  int get connectedClients => _sessions.length;

  /// Forcibly closes every active session with [code] and [reason]. Used by
  /// retry tests that need the client to observe a transport-level
  /// disconnect originating from the server.
  Future<void> killActiveConnections({
    int code = 4499,
    String reason = 'kill',
  }) async {
    for (final s in _sessions.toList()) {
      await s.close(code, reason);
    }
  }

  /// Stops accepting new connections, closes all open sessions, and shuts
  /// down the HTTP server.
  Future<void> dispose() async {
    for (final s in _sessions.toList()) {
      await s.close(1000, 'server shutdown');
    }
    await _httpServer.close(force: true);
  }
}

/// Per-connection protocol state.
class _Session {
  _Session(this._channel, this._server);

  final WebSocketChannel _channel;
  final GraphqlWsTestServer _server;
  final Map<String, StreamSubscription<Map<String, Object?>>> _subs = {};
  bool _initialised = false;
  bool _closed = false;
  void Function()? onClose;

  void start() {
    _channel.stream.listen(
      _onFrame,
      onDone: _onDone,
      onError: (Object _) => _onDone(),
    );
  }

  void _onFrame(Object? frame) {
    if (frame is! String) {
      _close(4400, 'expected text frame');
      return;
    }
    final Map<String, Object?> msg;
    try {
      final decoded = jsonDecode(frame);
      if (decoded is! Map<String, Object?>) {
        _close(4400, 'message must be a JSON object');
        return;
      }
      msg = decoded;
    } on FormatException {
      _close(4400, 'invalid JSON');
      return;
    }
    final type = msg['type'];
    if (type is! String) {
      _close(4400, 'missing type');
      return;
    }
    switch (type) {
      case 'connection_init':
        if (_initialised) {
          _close(4429, 'too many initialisation requests');
          return;
        }
        // Mark before the async gap so a second connection_init arriving
        // while the handler awaits is still rejected.
        _initialised = true;
        final rawPayload = msg['payload'];
        final payload = rawPayload is Map<String, Object?> ? rawPayload : null;
        unawaited(_processInit(payload));
      case 'ping':
        if (_server.respondToPings) {
          _send({
            'type': 'pong',
            if (msg['payload'] != null) 'payload': msg['payload'],
          });
        }
      case 'pong':
        _server._receivedPongCount++;
        break;
      case 'subscribe':
        if (!_initialised) {
          _close(4401, 'unauthorized');
          return;
        }
        _handleSubscribe(msg);
      case 'complete':
        final id = msg['id'];
        if (id is String) {
          unawaited(_subs.remove(id)?.cancel());
        }
      default:
        _close(4400, 'unknown message type: $type');
    }
  }

  Future<void> _processInit(Map<String, Object?>? payload) async {
    final rejectCode = await _server.onConnectionInit?.call(payload);
    if (rejectCode != null) {
      _close(rejectCode, 'rejected by onConnectionInit');
      return;
    }
    _send({'type': 'connection_ack'});
  }

  void _handleSubscribe(Map<String, Object?> msg) {
    final id = msg['id'];
    if (id is! String) {
      _close(4400, 'subscribe missing id');
      return;
    }
    if (_subs.containsKey(id)) {
      _close(4409, 'subscriber already exists');
      return;
    }
    final payload = msg['payload'];
    if (payload is! Map<String, Object?>) {
      _close(4400, 'subscribe missing payload');
      return;
    }
    _server.onSubscribe?.call(id, payload);
    final opName = payload['operationName'];
    final handler = opName is String ? _server._operations[opName] : null;
    if (handler == null) {
      _send({
        'id': id,
        'type': 'error',
        'payload': [
          {'message': 'no handler registered for operationName: $opName'},
        ],
      });
      return;
    }
    final Stream<Map<String, Object?>> stream;
    try {
      stream = handler(payload);
    } catch (e) {
      _send({
        'id': id,
        'type': 'error',
        'payload': [
          {'message': e.toString()},
        ],
      });
      return;
    }
    _subs[id] = stream.listen(
      (data) => _send({'id': id, 'type': 'next', 'payload': data}),
      onError: (Object err) {
        final List<Object?> errors;
        if (err is List) {
          errors = err.cast<Object?>();
        } else {
          errors = [
            {'message': err.toString()},
          ];
        }
        _send({'id': id, 'type': 'error', 'payload': errors});
        _subs.remove(id);
      },
      onDone: () {
        _send({'id': id, 'type': 'complete'});
        _subs.remove(id);
      },
    );
  }

  void sendPing({Map<String, Object?>? payload}) {
    _send({'type': 'ping', if (payload != null) 'payload': payload});
  }

  void _send(Map<String, Object?> msg) {
    if (_closed) return;
    _channel.sink.add(jsonEncode(msg));
  }

  void _close(int code, String reason) {
    if (_closed) return;
    _closed = true;
    unawaited(_channel.sink.close(code, reason));
  }

  Future<void> close(int code, String reason) async {
    _close(code, reason);
    await _drain();
  }

  void _onDone() {
    if (_closed) return;
    _closed = true;
    unawaited(_drain());
    onClose?.call();
  }

  Future<void> _drain() async {
    final pending = _subs.values.toList();
    _subs.clear();
    for (final s in pending) {
      await s.cancel();
    }
  }
}
