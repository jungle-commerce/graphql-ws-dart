import 'dart:async';

import 'package:graphql_ws/graphql_ws.dart';

/// A [Client] test double that records `ping`/`terminate` calls and lets
/// tests dispatch events to registered listeners.
///
/// `subscribe`/`stream` are unimplemented — `GraphqlWsConnectionManager`
/// never calls them.
class SpyClient implements Client {
  /// Number of times [terminate] has been called.
  int terminateCount = 0;

  /// The payload of every [ping] call, in order.
  final List<Map<String, Object?>?> pings = [];

  /// Whether [dispose] has been called.
  bool disposed = false;

  final List<void Function(ClientEvent)> _listeners = [];

  @override
  void Function() on<E extends ClientEvent>(ClientEventListener<E> listener) {
    void wrapper(ClientEvent event) {
      if (event is E) listener(event);
    }

    _listeners.add(wrapper);
    return () => _listeners.remove(wrapper);
  }

  /// Test helper: dispatches [event] to every registered listener.
  void emit(ClientEvent event) {
    for (final l in List.of(_listeners)) {
      l(event);
    }
  }

  /// Test helper: dispatches a server `Pong`.
  void emitPong() => emit(const PongEvent(received: true));

  /// Test helper: dispatches a (re)connection event.
  void emitConnected() => emit(ConnectedEvent(
        socket: _NullAdapter(),
        payload: null,
        wasRetry: true,
      ));

  @override
  void ping([Map<String, Object?>? payload]) => pings.add(payload);

  @override
  void terminate() => terminateCount++;

  @override
  Future<void> dispose() async => disposed = true;

  @override
  void Function() subscribe<TData extends Object?, TExtensions extends Object?>(
    SubscribePayload payload,
    GraphqlSink<FormattedExecutionResult<TData, TExtensions>> sink,
  ) =>
      throw UnimplementedError('SpyClient does not support subscribe');

  @override
  Stream<FormattedExecutionResult<TData, TExtensions>>
      stream<TData extends Object?, TExtensions extends Object?>(
    SubscribePayload payload,
  ) =>
          throw UnimplementedError('SpyClient does not support stream');
}

/// A no-op [WebSocketAdapter] — only needed to populate a [ConnectedEvent].
class _NullAdapter implements WebSocketAdapter {
  @override
  Stream<String> get messages => const Stream<String>.empty();

  @override
  Future<WebSocketCloseEvent> get done =>
      Completer<WebSocketCloseEvent>().future;

  @override
  bool get isOpen => true;

  @override
  void sendText(String data) {}

  @override
  void close([int? code, String? reason]) {}
}
