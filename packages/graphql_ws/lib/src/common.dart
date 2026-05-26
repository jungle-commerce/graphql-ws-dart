/// Common protocol types, messages, and (de)serialization for the
/// [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md).
///
/// Faithful port of `src/common.ts` from the JavaScript `graphql-ws` package.
library;

import 'dart:convert';

/// The WebSocket sub-protocol used for the
/// [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md).
///
/// This is the **only** sub-protocol this client offers during the WebSocket
/// handshake. The legacy `graphql-ws` sub-protocol used by the long-deprecated
/// [`subscriptions-transport-ws`](https://github.com/apollographql/subscriptions-transport-ws)
/// package is intentionally not supported — servers that only speak the
/// legacy protocol will fail the handshake with HTTP 400 / WebSocket close
/// code `1002`. Either upgrade the server to a `graphql-transport-ws`-compliant
/// implementation, or use a different client.
const String graphqlTransportWsProtocol = 'graphql-transport-ws';

/// `graphql-ws` expected and standard close codes of the
/// [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md).
enum CloseCode {
  internalServerError(4500),
  internalClientError(4005),
  badRequest(4400),
  badResponse(4004),

  /// Tried subscribing before connect ack.
  unauthorized(4401),
  forbidden(4403),
  subprotocolNotAcceptable(4406),
  connectionInitialisationTimeout(4408),
  connectionAcknowledgementTimeout(4504),

  /// Subscriber distinction is very important.
  subscriberAlreadyExists(4409),
  tooManyInitialisationRequests(4429);

  const CloseCode(this.code);

  /// The numeric close code dispatched over the wire.
  final int code;

  /// Returns the [CloseCode] whose [code] equals [value], or `null` if none.
  static CloseCode? fromCode(int value) {
    for (final c in CloseCode.values) {
      if (c.code == value) return c;
    }
    return null;
  }
}

/// Types of messages allowed to be sent by the client/server over the WS protocol.
enum MessageType {
  connectionInit('connection_init'), // Client -> Server
  connectionAck('connection_ack'), // Server -> Client
  ping('ping'), // bidirectional
  pong('pong'), // bidirectional
  subscribe('subscribe'), // Client -> Server
  next('next'), // Server -> Client
  error('error'), // Server -> Client
  complete('complete'); // bidirectional

  const MessageType(this.wireValue);

  /// The string value used on the wire (snake_case per protocol).
  final String wireValue;

  /// Resolves a [MessageType] from its wire value, or `null` if unknown.
  static MessageType? fromWire(String value) {
    for (final t in MessageType.values) {
      if (t.wireValue == value) return t;
    }
    return null;
  }
}

/// Something that can be disposed.
abstract interface class Disposable {
  /// Dispose of the instance and clear up resources.
  Future<void> dispose();
}

/// A representation of any set of values over any amount of time.
///
/// Mirrors the `Sink` interface from the JavaScript `graphql-ws` client. The
/// `Graphql` prefix avoids the name clash with `dart:async`'s `Sink` (which
/// uses `add`/`close`); the method names here match the JS contract.
///
/// Most Dart users should prefer [Client.stream] instead, which adapts this
/// onto a regular `Stream<T>`.
abstract interface class GraphqlSink<T> {
  /// Next value arriving.
  void next(T value);

  /// An error that has occurred. Calling this function "closes" the sink.
  ///
  /// The error can be a [GraphQL formatted error list](GraphQLFormattedError),
  /// an [Error]/[Exception], or a close-event-like object — assert during
  /// implementation.
  void error(Object error);

  /// The sink has completed. This function "closes" the sink.
  void complete();
}

/// The payload sent by the client when initiating a subscription.
class SubscribePayload {
  const SubscribePayload({
    required this.query,
    this.operationName,
    this.variables,
    this.extensions,
  });

  /// The GraphQL document to execute.
  final String query;

  /// Name of the operation to execute when [query] contains multiple operations.
  final String? operationName;

  /// Variables passed to the operation.
  final Map<String, Object?>? variables;

  /// Optional protocol extensions.
  final Map<String, Object?>? extensions;

  /// JSON representation matching the wire format.
  Map<String, Object?> toJson() => <String, Object?>{
        if (operationName != null) 'operationName': operationName,
        'query': query,
        if (variables != null) 'variables': variables,
        if (extensions != null) 'extensions': extensions,
      };

  /// Parses a [SubscribePayload] from a decoded JSON map.
  ///
  /// Throws [FormatException] if the input does not satisfy the protocol.
  factory SubscribePayload.fromJson(Map<String, Object?> json) {
    final query = json['query'];
    if (query is! String) {
      throw const FormatException(
          "SubscribePayload requires a 'query' property of type String");
    }
    final operationName = json['operationName'];
    if (operationName != null && operationName is! String) {
      throw const FormatException(
          "SubscribePayload 'operationName' must be a String, null, or absent");
    }
    final variables = json['variables'];
    if (variables != null && variables is! Map) {
      throw const FormatException(
          "SubscribePayload 'variables' must be a Map, null, or absent");
    }
    final extensions = json['extensions'];
    if (extensions != null && extensions is! Map) {
      throw const FormatException(
          "SubscribePayload 'extensions' must be a Map, null, or absent");
    }
    return SubscribePayload(
      query: query,
      operationName: operationName as String?,
      variables: (variables as Map?)?.cast<String, Object?>(),
      extensions: (extensions as Map?)?.cast<String, Object?>(),
    );
  }
}

/// A single source location inside a GraphQL document, as reported in errors.
class SourceLocation {
  const SourceLocation({required this.line, required this.column});

  final int line;
  final int column;

  Map<String, Object?> toJson() => <String, Object?>{
        'line': line,
        'column': column,
      };

  factory SourceLocation.fromJson(Map<String, Object?> json) {
    final line = json['line'];
    final column = json['column'];
    if (line is! int || column is! int) {
      throw const FormatException(
          'SourceLocation requires integer line and column');
    }
    return SourceLocation(line: line, column: column);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SourceLocation && line == other.line && column == other.column;

  @override
  int get hashCode => Object.hash(line, column);

  @override
  String toString() => 'SourceLocation(line: $line, column: $column)';
}

/// A GraphQL error in the formatted (serialised) shape.
///
/// See: https://spec.graphql.org/draft/#sec-Errors.
class GraphQLFormattedError {
  const GraphQLFormattedError({
    required this.message,
    this.locations,
    this.path,
    this.extensions,
  });

  /// A human-readable description of the error.
  final String message;

  /// Source locations within the query that triggered the error.
  final List<SourceLocation>? locations;

  /// The response path that produced the error. Each element is a [String]
  /// (field name) or [int] (list index).
  final List<Object>? path;

  /// Optional protocol-specific error metadata.
  final Map<String, Object?>? extensions;

  Map<String, Object?> toJson() => <String, Object?>{
        'message': message,
        if (locations != null)
          'locations':
              locations!.map((l) => l.toJson()).toList(growable: false),
        if (path != null) 'path': path,
        if (extensions != null) 'extensions': extensions,
      };

  factory GraphQLFormattedError.fromJson(Map<String, Object?> json) {
    final message = json['message'];
    if (message is! String) {
      throw const FormatException(
          "GraphQLFormattedError requires a 'message' property of type String");
    }
    final locationsRaw = json['locations'];
    final pathRaw = json['path'];
    final extensions = json['extensions'];
    return GraphQLFormattedError(
      message: message,
      locations: locationsRaw is List
          ? locationsRaw
              .whereType<Map<Object?, Object?>>()
              .map((m) => SourceLocation.fromJson(m.cast<String, Object?>()))
              .toList(growable: false)
          : null,
      path: pathRaw is List ? pathRaw.cast<Object>() : null,
      extensions: extensions is Map ? extensions.cast<String, Object?>() : null,
    );
  }

  @override
  String toString() =>
      'GraphQLFormattedError(message: $message${path != null ? ', path: $path' : ''})';
}

/// The formatted execution result of a GraphQL operation, as it appears
/// in `Next` messages.
///
/// Preserves the raw JSON payload so the `data: null` vs `data: absent`
/// distinction (meaningful in GraphQL) is not lost.
class FormattedExecutionResult<TData extends Object?,
    TExtensions extends Object?> {
  /// Construct from a decoded JSON payload (used during message parsing).
  const FormattedExecutionResult.fromJson(this.json);

  /// Convenience constructor for tests and adapters that need to build a
  /// result from typed fields.
  factory FormattedExecutionResult({
    TData? data,
    List<GraphQLFormattedError>? errors,
    bool? hasNext,
    TExtensions? extensions,
    bool includeNullData = false,
  }) {
    return FormattedExecutionResult<TData, TExtensions>.fromJson(
      <String, Object?>{
        if (data != null || includeNullData) 'data': data,
        if (errors != null)
          'errors': errors.map((e) => e.toJson()).toList(growable: false),
        if (hasNext != null) 'hasNext': hasNext,
        if (extensions != null) 'extensions': extensions,
      },
    );
  }

  /// The raw payload as received over the wire.
  final Map<String, Object?> json;

  /// Whether the `data` field is present in the payload (even if its value
  /// is `null`). GraphQL distinguishes `{data: null, errors: [...]}` (a field
  /// error nulled the root) from `{errors: [...]}` (parse/validation failure).
  bool get hasData => json.containsKey('data');

  /// The `data` field of the result, cast to [TData]. Returns `null` if the
  /// field is absent or its value is null.
  TData? get data => json['data'] as TData?;

  /// Errors reported by the server, if any.
  List<GraphQLFormattedError>? get errors {
    final raw = json['errors'];
    if (raw is! List) return null;
    return raw
        .whereType<Map<Object?, Object?>>()
        .map((m) => GraphQLFormattedError.fromJson(m.cast<String, Object?>()))
        .toList(growable: false);
  }

  /// Whether the server has more results to send for incremental delivery.
  bool? get hasNext => json['hasNext'] as bool?;

  /// Protocol extensions on the result envelope.
  TExtensions? get extensions => json['extensions'] as TExtensions?;

  /// JSON representation matching the wire format.
  Map<String, Object?> toJson() => json;

  @override
  String toString() => 'FormattedExecutionResult($json)';
}

/// Base class for all messages exchanged over the GraphQL WebSocket protocol.
sealed class Message {
  const Message();

  /// The message [type] tag.
  MessageType get type;

  /// Serialise this message to the wire JSON shape.
  Map<String, Object?> toJson();
}

/// Indicates that the client wants to establish a connection within the
/// existing socket.
class ConnectionInitMessage extends Message {
  const ConnectionInitMessage({this.payload});

  final Map<String, Object?>? payload;

  @override
  MessageType get type => MessageType.connectionInit;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'type': type.wireValue,
        if (payload != null) 'payload': payload,
      };
}

/// Server acknowledgement of the client's `ConnectionInit`.
class ConnectionAckMessage extends Message {
  const ConnectionAckMessage({this.payload});

  final Map<String, Object?>? payload;

  @override
  MessageType get type => MessageType.connectionAck;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'type': type.wireValue,
        if (payload != null) 'payload': payload,
      };
}

/// Bidirectional liveness probe; the receiver must respond with [PongMessage].
class PingMessage extends Message {
  const PingMessage({this.payload});

  final Map<String, Object?>? payload;

  @override
  MessageType get type => MessageType.ping;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'type': type.wireValue,
        if (payload != null) 'payload': payload,
      };
}

/// Response to [PingMessage]; may also be sent unsolicited as a heartbeat.
class PongMessage extends Message {
  const PongMessage({this.payload});

  final Map<String, Object?>? payload;

  @override
  MessageType get type => MessageType.pong;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'type': type.wireValue,
        if (payload != null) 'payload': payload,
      };
}

/// Requests an operation; results bind to [id].
class SubscribeMessage extends Message {
  const SubscribeMessage({required this.id, required this.payload});

  final String id;
  final SubscribePayload payload;

  @override
  MessageType get type => MessageType.subscribe;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'type': type.wireValue,
        'payload': payload.toJson(),
      };
}

/// A single result delivered for the subscription identified by [id].
class NextMessage extends Message {
  const NextMessage({required this.id, required this.payload});

  final String id;
  final FormattedExecutionResult<Map<String, Object?>, Map<String, Object?>>
      payload;

  @override
  MessageType get type => MessageType.next;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'type': type.wireValue,
        'payload': payload.toJson(),
      };
}

/// Operation execution error(s) terminating the subscription identified by [id].
class ErrorMessage extends Message {
  const ErrorMessage({required this.id, required this.payload});

  final String id;
  final List<GraphQLFormattedError> payload;

  @override
  MessageType get type => MessageType.error;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'type': type.wireValue,
        'payload': payload.map((e) => e.toJson()).toList(growable: false),
      };
}

/// Indicates the subscription identified by [id] has completed.
class CompleteMessage extends Message {
  const CompleteMessage({required this.id});

  final String id;

  @override
  MessageType get type => MessageType.complete;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'type': type.wireValue,
      };
}

/// Function for transforming values within a message during JSON parsing.
///
/// Passed through to `dart:convert`'s [`jsonDecode`] `reviver` parameter and
/// invoked for every key/value pair produced by the parser.
typedef JsonMessageReviver = Object? Function(Object? key, Object? value);

/// Function for transforming values within a message during JSON encoding.
///
/// Passed through to `dart:convert`'s [`jsonEncode`] `toEncodable` parameter
/// and invoked for objects that the encoder cannot directly serialise.
///
/// Note: this differs from the JS `replacer` semantics (which is invoked for
/// every key); Dart's `toEncodable` is only invoked for non-encodable values.
typedef JsonMessageReplacer = Object? Function(Object? nonEncodable);

/// Returns the extended `typeof`-style description of [val], used for
/// reporting validation failures with parity to the upstream JS messages.
String _extendedTypeof(Object? val) {
  if (val == null) return 'null';
  if (val is List) return 'array';
  if (val is bool) return 'boolean';
  if (val is num) return 'number';
  if (val is String) return 'string';
  if (val is Function) return 'function';
  if (val is Map) return 'object';
  return 'object';
}

/// Validates the [val] message against the GraphQL over WebSocket Protocol.
///
/// Invalid messages throw a [FormatException] with a descriptive error.
Message validateMessage(Object? val) {
  if (val is! Map) {
    throw FormatException(
        'Message is expected to be an object, but got ${_extendedTypeof(val)}');
  }
  // Normalise to Map<String, Object?>.
  final map = val.cast<String, Object?>();

  final rawType = map['type'];
  if (rawType == null) {
    throw const FormatException("Message is missing the 'type' property");
  }
  if (rawType is! String) {
    throw FormatException(
        "Message is expects the 'type' property to be a string, but got "
        '${_extendedTypeof(rawType)}');
  }

  final type = MessageType.fromWire(rawType);
  if (type == null) {
    throw FormatException("Invalid message 'type' property \"$rawType\"");
  }

  switch (type) {
    case MessageType.connectionInit:
    case MessageType.connectionAck:
    case MessageType.ping:
    case MessageType.pong:
      final payload = map['payload'];
      if (payload != null && payload is! Map) {
        throw FormatException(
            '"${type.wireValue}" message expects the \'payload\' property to '
            'be an object or nullish or missing, but got "$payload"');
      }
      final payloadMap = (payload as Map?)?.cast<String, Object?>();
      switch (type) {
        case MessageType.connectionInit:
          return ConnectionInitMessage(payload: payloadMap);
        case MessageType.connectionAck:
          return ConnectionAckMessage(payload: payloadMap);
        case MessageType.ping:
          return PingMessage(payload: payloadMap);
        case MessageType.pong:
          return PongMessage(payload: payloadMap);
        // ignore: no_default_cases
        default:
          throw StateError('unreachable');
      }

    case MessageType.subscribe:
      _requireNonEmptyId(map, type);
      final payload = map['payload'];
      if (payload is! Map) {
        throw FormatException(
            '"${type.wireValue}" message expects the \'payload\' property to '
            'be an object, but got ${_extendedTypeof(payload)}');
      }
      final payloadMap = payload.cast<String, Object?>();
      final query = payloadMap['query'];
      if (query is! String) {
        throw FormatException(
            '"${type.wireValue}" message payload expects the \'query\' '
            'property to be a string, but got ${_extendedTypeof(query)}');
      }
      final variables = payloadMap['variables'];
      if (variables != null && variables is! Map) {
        throw FormatException(
            '"${type.wireValue}" message payload expects the \'variables\' '
            'property to be a an object or nullish or missing, but got '
            '${_extendedTypeof(variables)}');
      }
      final operationName = payloadMap['operationName'];
      if (operationName != null && operationName is! String) {
        throw FormatException('"${type.wireValue}" message payload expects the '
            "'operationName' property to be a string or nullish or missing, "
            'but got ${_extendedTypeof(operationName)}');
      }
      final extensions = payloadMap['extensions'];
      if (extensions != null && extensions is! Map) {
        throw FormatException(
            '"${type.wireValue}" message payload expects the \'extensions\' '
            'property to be a an object or nullish or missing, but got '
            '${_extendedTypeof(extensions)}');
      }
      return SubscribeMessage(
        id: map['id']! as String,
        payload: SubscribePayload(
          query: query,
          operationName: operationName as String?,
          variables: (variables as Map?)?.cast<String, Object?>(),
          extensions: (extensions as Map?)?.cast<String, Object?>(),
        ),
      );

    case MessageType.next:
      _requireNonEmptyId(map, type);
      final payload = map['payload'];
      if (payload is! Map) {
        throw FormatException(
            '"${type.wireValue}" message expects the \'payload\' property to '
            'be an object, but got ${_extendedTypeof(payload)}');
      }
      return NextMessage(
        id: map['id']! as String,
        payload:
            FormattedExecutionResult.fromJson(payload.cast<String, Object?>()),
      );

    case MessageType.error:
      _requireNonEmptyId(map, type);
      final payload = map['payload'];
      if (payload is! List ||
          payload.isEmpty ||
          !payload.every((e) => e is Map && e.containsKey('message'))) {
        throw FormatException(
            '"${type.wireValue}" message expects the \'payload\' property to '
            'be an array of GraphQL errors, but got '
            '${jsonEncode(payload)}');
      }
      return ErrorMessage(
        id: map['id']! as String,
        payload: payload
            .cast<Map<Object?, Object?>>()
            .map((m) =>
                GraphQLFormattedError.fromJson(m.cast<String, Object?>()))
            .toList(growable: false),
      );

    case MessageType.complete:
      _requireNonEmptyId(map, type);
      return CompleteMessage(id: map['id']! as String);
  }
}

void _requireNonEmptyId(Map<String, Object?> map, MessageType type) {
  final id = map['id'];
  if (id is! String) {
    throw FormatException(
        '"${type.wireValue}" message expects the \'id\' property to be a '
        'string, but got ${_extendedTypeof(id)}');
  }
  if (id.isEmpty) {
    throw FormatException(
        '"${type.wireValue}" message requires a non-empty \'id\' property');
  }
}

/// Parses raw WebSocket message [data] (typically a JSON-encoded `String`)
/// into a validated [Message].
///
/// If [data] is already a decoded `Map`, it is validated directly.
Message parseMessage(Object? data, {JsonMessageReviver? reviver}) {
  final decoded = data is String ? jsonDecode(data, reviver: reviver) : data;
  return validateMessage(decoded);
}

/// Stringifies a valid [msg] into the wire JSON shape, ready to be sent
/// through the socket.
///
/// Throws [FormatException] if [msg] fails revalidation.
String stringifyMessage(Message msg, {JsonMessageReplacer? replacer}) {
  // Re-validate to catch impossible inline mutations (parity with JS impl).
  validateMessage(msg.toJson());
  return jsonEncode(msg.toJson(), toEncodable: replacer);
}
