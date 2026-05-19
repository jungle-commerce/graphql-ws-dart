/// Events emitted by the [Client] as its connection lifecycle progresses.
///
/// Mirrors the event taxonomy from the JavaScript `graphql-ws` client. Events
/// are exposed via [Client.on] using Dart 3 sealed-class pattern matching:
///
/// ```dart
/// final unlisten = client.on<ClientEvent>((event) {
///   switch (event) {
///     case ConnectingEvent(:final isRetry):
///       print('connecting (retry=$isRetry)');
///     case ConnectedEvent(:final wasRetry):
///       print('connected (recovered=$wasRetry)');
///     case ClosedEvent(:final code):
///       print('closed ($code)');
///     case ErrorEvent(:final error):
///       print('error: $error');
///     case _: break;
///   }
/// });
/// ```
library;

import 'common.dart';
import 'websocket_adapter.dart';

/// Base type for all client lifecycle events.
sealed class ClientEvent {
  const ClientEvent();
}

/// The client has begun a connection attempt.
class ConnectingEvent extends ClientEvent {
  const ConnectingEvent({required this.isRetry});

  /// Whether this attempt is a retry following a previous failure.
  final bool isRetry;
}

/// The underlying WebSocket has opened (handshake completed), but the
/// `ConnectionAck` has not been received yet.
class OpenedEvent extends ClientEvent {
  const OpenedEvent(this.socket);

  final WebSocketAdapter socket;
}

/// The server has acknowledged the connection (`ConnectionAck` received).
/// Subscriptions may now be requested.
class ConnectedEvent extends ClientEvent {
  const ConnectedEvent({
    required this.socket,
    required this.payload,
    required this.wasRetry,
  });

  final WebSocketAdapter socket;

  /// Optional payload supplied by the server in the `ConnectionAck` message.
  final Map<String, Object?>? payload;

  /// Whether this connection was established as a retry.
  final bool wasRetry;
}

/// A `PingMessage` was either received from the server (`received == true`)
/// or sent by the client (`received == false`).
class PingEvent extends ClientEvent {
  const PingEvent({required this.received, this.payload});

  final bool received;
  final Map<String, Object?>? payload;
}

/// A `PongMessage` was either received from the server (`received == true`)
/// or sent by the client (`received == false`).
class PongEvent extends ClientEvent {
  const PongEvent({required this.received, this.payload});

  final bool received;
  final Map<String, Object?>? payload;
}

/// A protocol [Message] was received by the client.
///
/// Emitted for **every** valid inbound message. Useful for debugging and
/// logging; subscription delivery still flows through the
/// [Sink]/[Stream] returned by `subscribe`/`stream`.
class MessageEvent extends ClientEvent {
  const MessageEvent(this.message);

  final Message message;
}

/// The WebSocket connection has closed.
class ClosedEvent extends ClientEvent {
  const ClosedEvent({this.code, this.reason});

  final int? code;
  final String? reason;
}

/// The WebSocket connection emitted an error, or an internal client error
/// has occurred.
class ErrorEvent extends ClientEvent {
  const ErrorEvent(this.error);

  final Object error;
}
