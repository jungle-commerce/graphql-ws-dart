/// Coherent, zero-dependency, lazy, simple, GraphQL over WebSocket Protocol
/// compliant client.
///
/// Dart port of the JavaScript [`graphql-ws`](https://github.com/enisdenjo/graphql-ws)
/// package. Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md)
/// (`graphql-transport-ws` sub-protocol — the only sub-protocol supported).
library graphql_ws;

export 'src/client.dart'
    show
        Client,
        ClientEventListener,
        RetryWait,
        ShouldRetry,
        SubscriptionIdGenerator,
        createClient;
export 'src/common.dart'
    show
        CloseCode,
        CompleteMessage,
        ConnectionAckMessage,
        ConnectionInitMessage,
        Disposable,
        ErrorMessage,
        FormattedExecutionResult,
        GraphqlSink,
        GraphQLFormattedError,
        JsonMessageReplacer,
        JsonMessageReviver,
        Message,
        MessageType,
        NextMessage,
        PingMessage,
        PongMessage,
        SourceLocation,
        SubscribeMessage,
        SubscribePayload,
        graphqlTransportWsProtocol,
        parseMessage,
        stringifyMessage,
        validateMessage;
export 'src/default_connector.dart';
export 'src/events.dart'
    show
        ClientEvent,
        ClosedEvent,
        ConnectedEvent,
        ConnectingEvent,
        ErrorEvent,
        MessageEvent,
        OpenedEvent,
        PingEvent,
        PongEvent;
export 'src/utils.dart' show LikeCloseEvent, TerminatedCloseEvent;
export 'src/websocket_adapter.dart'
    show WebSocketAdapter, WebSocketCloseEvent, WebSocketConnector;
