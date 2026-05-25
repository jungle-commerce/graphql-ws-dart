/// GraphQL over WebSocket Protocol client implementation.
///
/// Faithful port of `src/client.ts` from the JavaScript `graphql-ws` package.
/// The connect/retry/lock state machine, lazy semantics, keep-alive, and
/// `terminate()` behavior all mirror the upstream implementation.
library;

import 'dart:async';
import 'dart:math';

import 'common.dart';
import 'default_connector.dart';
import 'events.dart';
import 'utils.dart';
import 'websocket_adapter.dart';

/// Signature of a listener returned by [Client.on].
typedef ClientEventListener<E extends ClientEvent> = void Function(E event);

/// Function that produces an `id` for a new subscription. Defaults to a v4
/// UUID generator. Override to plug in your own scheme.
typedef SubscriptionIdGenerator = String Function(SubscribePayload payload);

/// Function controlling the back-off between connection retries.
///
/// [retries] counts actual reconnect attempts; it is `0` for the first
/// retry following a disconnect.
typedef RetryWait = Future<void> Function(int retries);

/// Predicate determining whether an error/close event should trigger a
/// retry. Note that some close codes are unconditionally fatal regardless
/// of this predicate (see [Client] documentation).
typedef ShouldRetry = bool Function(Object errOrCloseEvent);

/// Disposable GraphQL over WebSocket client.
///
/// A faithful port of the JavaScript [`graphql-ws`](https://github.com/enisdenjo/graphql-ws)
/// client. Construct via [createClient].
abstract interface class Client implements Disposable {
  /// Subscribes to client lifecycle events. Returns an unlisten function.
  ///
  /// Use a sealed-class pattern match to handle each event type:
  ///
  /// ```dart
  /// final off = client.on<ClientEvent>((event) {
  ///   switch (event) {
  ///     case ConnectedEvent(): /* ... */
  ///     case ClosedEvent(:final code): /* ... */
  ///     case _: break;
  ///   }
  /// });
  /// ```
  ///
  /// To listen for one specific event type, narrow [E]:
  ///
  /// ```dart
  /// client.on<ConnectedEvent>((e) => print(e.wasRetry));
  /// ```
  void Function() on<E extends ClientEvent>(ClientEventListener<E> listener);

  /// Subscribes through the WebSocket and delivers results to [sink].
  ///
  /// Returns a dispose function that cancels the subscription. Calling
  /// dispose will, if appropriate, send a `Complete` message to the server.
  void Function() subscribe<TData extends Object?, TExtensions extends Object?>(
    SubscribePayload payload,
    GraphqlSink<FormattedExecutionResult<TData, TExtensions>> sink,
  );

  /// Subscribes through the WebSocket and emits results as a `Stream`.
  ///
  /// Cancelling the returned stream's subscription disposes the underlying
  /// subscription (sends `Complete` to the server if still active).
  Stream<FormattedExecutionResult<TData, TExtensions>>
      stream<TData extends Object?, TExtensions extends Object?>(
    SubscribePayload payload,
  );

  /// Terminates the WebSocket abruptly and immediately.
  ///
  /// A synthetic [TerminatedCloseEvent] (code `4499`) is emitted without
  /// waiting for the underlying socket's close callback. Terminating is
  /// **not** considered fatal — the configured retry policy will run.
  ///
  /// Useful when the WebSocket has frozen and stopped emitting events
  /// (e.g. iOS Safari, see https://github.com/enisdenjo/graphql-ws/discussions/290).
  void terminate();

  /// Sends a `Ping` message over the active connection, optionally carrying
  /// [payload].
  ///
  /// A no-op when there is no live connection. A compliant server replies
  /// with a `Pong`, surfacing as a [PongEvent] with `received: true` — which
  /// makes this a useful on-demand liveness probe for detecting a frozen
  /// ("zombie") socket, e.g. after an app returns from the background.
  ///
  /// Distinct from the automatic [keepAlive] pings: this fires immediately,
  /// once, regardless of the keep-alive interval.
  void ping([Map<String, Object?>? payload]);
}

/// Creates a disposable GraphQL over WebSocket client.
///
/// All options are optional except [url]. See the docs on each parameter for
/// behavior details. The implementation mirrors the JavaScript
/// `createClient(...)` 1:1.
///
/// - [url]: Async-resolvable function returning the server URL. Called on
///   every connection attempt — useful for refreshed auth tokens.
/// - [connectionParams]: Async-resolvable function (or value) returning the
///   `payload` for the `ConnectionInit` message.
/// - [lazy]: When `true` (default), the connection is opened on first
///   subscribe and closed on last unsubscribe. When `false`, the connection
///   opens immediately and you must provide [onNonLazyError] to observe
///   unrecoverable errors.
/// - [lazyCloseTimeout]: Calm-down window before the connection is closed
///   after the last subscription completes.
/// - [keepAlive]: Interval between client-initiated `Ping` messages. The
///   client does **not** automatically tear down the connection if no
///   `Pong` arrives; use the `on` callbacks to implement your own watchdog.
/// - [connectionAckWaitTimeout]: Maximum time to wait for `ConnectionAck`
///   after sending `ConnectionInit`. Exceeding this closes the socket with
///   close code [CloseCode.connectionAcknowledgementTimeout].
/// - [disablePong]: Skip the automatic `Pong` response to inbound `Ping`s.
///   Useful when you want to customise the response payload.
/// - [retryAttempts]: How many reconnect attempts to make on non-fatal
///   disconnects before erroring out. Defaults to `5`.
/// - [retryWait]: Backoff implementation. Defaults to randomised
///   exponential backoff (`2^retries` seconds + 0.3-3s jitter).
/// - [shouldRetry]: Predicate to override which non-CloseEvent errors are
///   retryable. Defaults to: only retry close-event-like errors.
/// - [generateId]: Subscription id generator. Defaults to UUID v4.
/// - [jsonMessageReviver]: Reviver passed to `jsonDecode` for inbound frames.
/// - [connector]: Custom WebSocket transport. Defaults to
///   [defaultWebSocketConnector] (`dart:io.WebSocket`); supply a custom
///   connector for Flutter web or other environments.
/// - [onNonLazyError]: Required when [lazy] is `false`. Called once when
///   the non-lazy client errors out unrecoverably.
Client createClient({
  required FutureOr<Uri> Function() url,
  FutureOr<Map<String, Object?>?> Function()? connectionParams,
  bool lazy = true,
  void Function(Object errorOrCloseEvent)? onNonLazyError,
  Duration lazyCloseTimeout = Duration.zero,
  Duration keepAlive = Duration.zero,
  Duration connectionAckWaitTimeout = Duration.zero,
  bool disablePong = false,
  int retryAttempts = 5,
  RetryWait? retryWait,
  ShouldRetry? shouldRetry,
  SubscriptionIdGenerator? generateId,
  JsonMessageReviver? jsonMessageReviver,
  JsonMessageReplacer? jsonMessageReplacer,
  WebSocketConnector? connector,
}) {
  if (!lazy && onNonLazyError == null) {
    throw ArgumentError(
      'A non-lazy client (lazy: false) requires onNonLazyError to surface '
      'unrecoverable failures. Pass an explicit callback, or use the default '
      'lazy mode.',
    );
  }
  return _GraphqlWsClient(
    url: url,
    connectionParams: connectionParams,
    lazy: lazy,
    onNonLazyError: onNonLazyError,
    lazyCloseTimeout: lazyCloseTimeout,
    keepAlive: keepAlive,
    connectionAckWaitTimeout: connectionAckWaitTimeout,
    disablePong: disablePong,
    retryAttempts: retryAttempts,
    retryWait: retryWait ?? _defaultRetryWait,
    shouldRetry: shouldRetry ?? _defaultShouldRetry,
    generateId: generateId ?? ((_) => generateUuidV4()),
    jsonMessageReviver: jsonMessageReviver,
    jsonMessageReplacer: jsonMessageReplacer,
    connector: connector ?? defaultWebSocketConnector,
  );
}

/// Default randomised exponential backoff: `2^retries` seconds + 300-3000ms
/// of jitter.
Future<void> _defaultRetryWait(int retries) {
  final base = pow(2, retries).toInt();
  final jitter = 300 + Random().nextInt(2700);
  return Future<void>.delayed(Duration(seconds: base, milliseconds: jitter));
}

/// Default [ShouldRetry]: retry only on close-event-like failures.
bool _defaultShouldRetry(Object errOrCloseEvent) =>
    errOrCloseEvent is LikeCloseEvent;

class _GraphqlWsClient implements Client {
  _GraphqlWsClient({
    required FutureOr<Uri> Function() url,
    required this.connectionParams,
    required this.lazy,
    required this.onNonLazyError,
    required this.lazyCloseTimeout,
    required this.keepAlive,
    required this.connectionAckWaitTimeout,
    required this.disablePong,
    required this.retryAttempts,
    required this.retryWait,
    required this.shouldRetry,
    required this.generateId,
    required this.jsonMessageReviver,
    required this.jsonMessageReplacer,
    required this.connector,
  }) : _url = url {
    if (!lazy) {
      _startNonLazyConnect();
    }
  }

  // ------------------------------------------------------------------
  // Config (immutable after construction).
  // ------------------------------------------------------------------

  final FutureOr<Uri> Function() _url;
  final FutureOr<Map<String, Object?>?> Function()? connectionParams;
  final bool lazy;
  final void Function(Object errorOrCloseEvent)? onNonLazyError;
  final Duration lazyCloseTimeout;
  final Duration keepAlive;
  final Duration connectionAckWaitTimeout;
  final bool disablePong;
  final int retryAttempts;
  final RetryWait retryWait;
  final ShouldRetry shouldRetry;
  final SubscriptionIdGenerator generateId;
  final JsonMessageReviver? jsonMessageReviver;
  final JsonMessageReplacer? jsonMessageReplacer;
  final WebSocketConnector connector;

  // ------------------------------------------------------------------
  // Listener registry.
  // ------------------------------------------------------------------

  final List<ClientEventListener<ClientEvent>> _listeners = [];
  final Map<String, void Function(Message message)> _messageById = {};

  // ------------------------------------------------------------------
  // Mutable connection state.
  // ------------------------------------------------------------------

  Future<_Connected>? _connecting;
  WebSocketAdapter? _activeAdapter;
  _ConnectionAttempt? _activeAttempt;
  int _locks = 0;
  Timer? _lazyCloseTimer;
  bool _retrying = false;
  int _retries = 0;
  bool _disposed = false;

  // ------------------------------------------------------------------
  // Public API.
  // ------------------------------------------------------------------

  @override
  void Function() on<E extends ClientEvent>(
    ClientEventListener<E> listener,
  ) {
    void wrapper(ClientEvent event) {
      if (event is E) listener(event);
    }

    _listeners.add(wrapper);
    return () => _listeners.remove(wrapper);
  }

  @override
  void Function()
      subscribe<TData extends Object?, TExtensions extends Object?>(
    SubscribePayload payload,
    GraphqlSink<FormattedExecutionResult<TData, TExtensions>> sink,
  ) {
    final id = generateId(payload);
    final state = _SubscriptionState();

    // Initial releaser: handles the "completed before connect" case.
    state.releaser = () {
      _locks--;
      state.done = true;
    };

    unawaited(_runSubscription<TData, TExtensions>(id, payload, sink, state)
        .then((_) {
      if (!state.errored) sink.complete();
    }).catchError((Object err) {
      sink.error(err);
    }));

    return () {
      if (!state.done) state.releaser();
    };
  }

  @override
  Stream<FormattedExecutionResult<TData, TExtensions>>
      stream<TData extends Object?, TExtensions extends Object?>(
    SubscribePayload payload,
  ) {
    late StreamController<FormattedExecutionResult<TData, TExtensions>>
        controller;
    void Function()? dispose;

    controller = StreamController<FormattedExecutionResult<TData, TExtensions>>(
      onListen: () {
        dispose = subscribe<TData, TExtensions>(
          payload,
          _ControllerSink<FormattedExecutionResult<TData, TExtensions>>(
              controller),
        );
      },
      onCancel: () {
        dispose?.call();
      },
    );

    return controller.stream;
  }

  @override
  Future<void> dispose() async {
    _disposed = true;
    // Close the live socket eagerly so a mid-handshake dispose doesn't hang
    // waiting on an ack that will never arrive.
    final active = _activeAdapter;
    if (active != null && active.isOpen) {
      active.close(1000, 'Normal Closure');
    }
    final connecting = _connecting;
    if (connecting != null) {
      try {
        final connected = await connecting;
        if (connected.adapter.isOpen) {
          connected.adapter.close(1000, 'Normal Closure');
        }
        try {
          await connected.adapter.done;
        } on Object {
          // Swallow — close path may surface errors that are not relevant
          // post-dispose.
        }
      } on Object {
        // Connection never completed; nothing to tear down.
      }
    }
    // Drop any leftover active reference.
    _activeAdapter = null;
  }

  @override
  void terminate() {
    final attempt = _activeAttempt;
    if (attempt == null) return;
    // Route through the synthetic Terminated close. The same failure handler
    // emits ClosedEvent(4499, "Terminated"), tears down the attempt, and
    // (best-effort) closes the underlying adapter even if it has frozen.
    attempt.failureHandler?.call(const TerminatedCloseEvent());
  }

  @override
  void ping([Map<String, Object?>? payload]) {
    final adapter = _activeAdapter;
    if (adapter == null || !adapter.isOpen) return;
    adapter.sendText(stringifyMessage(
      PingMessage(payload: payload),
      replacer: jsonMessageReplacer,
    ));
    _emit(PingEvent(received: false, payload: payload));
  }

  // ------------------------------------------------------------------
  // Internals.
  // ------------------------------------------------------------------

  void _emit(ClientEvent event) {
    // Snapshot listeners so removals during emit don't shift indices.
    for (final l in List.of(_listeners)) {
      try {
        l(event);
      } on Object {
        // Listener errors are not propagated to peers.
      }
    }
    if (event is MessageEvent) {
      final m = event.message;
      final id = switch (m) {
        SubscribeMessage(:final id) => id,
        NextMessage(:final id) => id,
        ErrorMessage(:final id) => id,
        CompleteMessage(:final id) => id,
        _ => null,
      };
      if (id != null) {
        _messageById[id]?.call(m);
      }
    }
  }

  void Function() _listenForMessageId(
      String id, void Function(Message) listener) {
    _messageById[id] = listener;
    return () {
      if (identical(_messageById[id], listener)) {
        _messageById.remove(id);
      }
    };
  }

  /// Establish (or reuse) a connection. Returns the adapter plus a "release"
  /// function the caller invokes when it no longer needs the connection, and
  /// a future that completes (normally or with error) when the connection
  /// is no longer usable for this subscriber.
  Future<_Released> _connect() async {
    // Clear any pending lazy-close so a new subscribe debounces it.
    _lazyCloseTimer?.cancel();
    _lazyCloseTimer = null;

    final connectingFuture = _connecting ??= _startConnect();
    late _Connected connected;
    try {
      connected = await connectingFuture;
    } on Object {
      rethrow;
    }

    // The previously-shared connection may have just begun closing (last
    // subscriber dropped, lazy-close decided to close, etc.). In that case
    // we wait for the close to finish, then loop back through `_connect()`
    // so a fresh attempt is started.
    if (!connected.adapter.isOpen) {
      try {
        await connected.throwOnClose;
      } on Object {
        // Expected — throwOnClose fires with the close event.
      }
      return _connect();
    }

    // Per-subscriber release plumbing.
    final releasedCompleter = Completer<void>();
    void release() {
      if (!releasedCompleter.isCompleted) releasedCompleter.complete();
    }

    final waitForReleaseOrClose = Future.any(<Future<void>>[
      releasedCompleter.future.then((_) {
        if (_locks == 0) {
          void complete() {
            if (connected.adapter.isOpen) {
              connected.adapter.close(1000, 'Normal Closure');
            }
          }

          if (lazyCloseTimeout > Duration.zero) {
            _lazyCloseTimer = Timer(lazyCloseTimeout, () {
              if (connected.adapter.isOpen) complete();
            });
          } else {
            complete();
          }
        }
      }),
      connected.throwOnClose,
    ]);

    return _Released(
      adapter: connected.adapter,
      release: release,
      waitForReleaseOrClose: waitForReleaseOrClose,
    );
  }

  Future<_Connected> _startConnect() {
    final attempt = _ConnectionAttempt();
    // Track this attempt before the inner async runs so an early dispose
    // can short-circuit it (otherwise we'd hang awaiting `connector`).
    _activeAttempt = attempt;

    Future<void>(() async {
      if (_retrying) {
        await retryWait(_retries);
        if (_locks == 0) {
          _connecting = null;
          attempt.failConnecting(const LikeCloseEvent(
              code: 1000, reason: 'All Subscriptions Gone'));
          return;
        }
        _retries++;
      }

      _emit(ConnectingEvent(isRetry: _retrying));

      final WebSocketAdapter adapter;
      try {
        final uri = await _url();
        adapter = await connector(uri, const [graphqlTransportWsProtocol]);
      } on Object catch (err) {
        _connecting = null;
        if (identical(_activeAttempt, attempt)) _activeAttempt = null;
        attempt.failConnecting(err);
        return;
      }

      if (_disposed) {
        // dispose() was called while we were waiting for the WebSocket
        // handshake. Tear down the freshly-opened socket and fail this
        // attempt so the caller's pending futures complete.
        adapter.close(1000, 'Normal Closure');
        _connecting = null;
        if (identical(_activeAttempt, attempt)) _activeAttempt = null;
        attempt.failConnecting(
            const LikeCloseEvent(code: 1000, reason: 'Disposed'));
        return;
      }

      attempt.adapter = adapter;
      _activeAdapter = adapter;
      _activeAttempt = attempt;

      Timer? ackTimer;
      Timer? pingTimer;

      void enqueuePing() {
        if (keepAlive == Duration.zero || !keepAlive.isFinite) return;
        pingTimer?.cancel();
        pingTimer = Timer(keepAlive, () {
          if (adapter.isOpen) {
            adapter.sendText(stringifyMessage(const PingMessage(),
                replacer: jsonMessageReplacer));
            _emit(const PingEvent(received: false));
          }
        });
      }

      void onSocketFailure(Object errOrEvent) {
        if (attempt.handled) return;
        attempt.handled = true;

        // Translate transport close events to the LikeCloseEvent shape used
        // by the retry classifier and the sink error pipeline. `null` code
        // (e.g. abrupt disconnect with no status frame) maps to 1005 per the
        // WebSocket spec.
        final Object normalised = errOrEvent is WebSocketCloseEvent
            ? LikeCloseEvent(
                code: errOrEvent.code ?? 1005,
                reason: errOrEvent.reason ?? '',
              )
            : errOrEvent;

        _connecting = null;
        if (identical(_activeAdapter, adapter)) _activeAdapter = null;
        if (identical(_activeAttempt, attempt)) _activeAttempt = null;
        ackTimer?.cancel();
        pingTimer?.cancel();

        // Emit lifecycle events before propagating the failure so listeners
        // observe state changes in the expected order.
        if (normalised is LikeCloseEvent) {
          _emit(ClosedEvent(
              code: normalised.code, reason: normalised.reason));
        } else {
          _emit(ErrorEvent(normalised));
          _emit(ClosedEvent(code: null, reason: normalised.toString()));
        }

        attempt.fail(normalised);
        if (errOrEvent is TerminatedCloseEvent) {
          // The synthetic Terminated event tears down the socket without
          // waiting for the native close handshake — see JS impl.
          try {
            adapter.close(4499, 'Terminated');
          } on Object {
            // ignore — adapter may already be closing
          }
        }
      }

      // Surface this attempt's failure path to external callers like
      // `terminate()`.
      attempt.failureHandler = onSocketFailure;

      // Forward the adapter's `done` future into the failure pipeline.
      unawaited(adapter.done.then((event) {
        // If we already considered the connection finished, ignore.
        onSocketFailure(event);
      }, onError: (Object err) {
        onSocketFailure(err);
      }));

      // onopen analogue: send ConnectionInit, schedule ack timeout, schedule ping.
      try {
        _emit(OpenedEvent(adapter));
        final Map<String, Object?>? payload;
        if (connectionParams != null) {
          payload = await connectionParams!();
        } else {
          payload = null;
        }

        // connectionParams may have taken long enough for the socket to die.
        if (!adapter.isOpen) return;

        adapter.sendText(stringifyMessage(
          payload != null
              ? ConnectionInitMessage(payload: payload)
              : const ConnectionInitMessage(),
          replacer: jsonMessageReplacer,
        ));

        if (connectionAckWaitTimeout > Duration.zero &&
            connectionAckWaitTimeout.isFinite) {
          ackTimer = Timer(connectionAckWaitTimeout, () {
            adapter.close(
              CloseCode.connectionAcknowledgementTimeout.code,
              'Connection acknowledgement timeout',
            );
          });
        }

        enqueuePing();
      } on Object catch (err) {
        _emit(ErrorEvent(err));
        adapter.close(
          CloseCode.internalClientError.code,
          limitCloseReason(err.toString(), 'Internal client error'),
        );
        return;
      }

      // onmessage analogue.
      attempt.messagesSubscription =
          adapter.messages.listen((String raw) {
        try {
          final message = parseMessage(raw, reviver: jsonMessageReviver);
          _emit(MessageEvent(message));

          if (message is PingMessage || message is PongMessage) {
            if (message is PingMessage) {
              _emit(PingEvent(received: true, payload: message.payload));
              if (!disablePong) {
                adapter.sendText(stringifyMessage(
                  message.payload != null
                      ? PongMessage(payload: message.payload)
                      : const PongMessage(),
                  replacer: jsonMessageReplacer,
                ));
                _emit(PongEvent(received: false, payload: message.payload));
              }
            } else if (message is PongMessage) {
              _emit(PongEvent(received: true, payload: message.payload));
              enqueuePing();
            }
            return;
          }

          if (attempt.acknowledged) return;

          if (message is! ConnectionAckMessage) {
            throw FormatException(
                'First message cannot be of type ${message.type.wireValue}');
          }
          ackTimer?.cancel();
          attempt.acknowledged = true;
          _emit(ConnectedEvent(
            socket: adapter,
            payload: message.payload,
            wasRetry: _retrying,
          ));
          _retrying = false;
          _retries = 0;
          attempt.completeConnected();
        } on Object catch (err) {
          // Stop reading messages — subsequent frames may be invalid too.
          unawaited(attempt.messagesSubscription?.cancel());
          attempt.messagesSubscription = null;
          _emit(ErrorEvent(err));
          adapter.close(
            CloseCode.badResponse.code,
            limitCloseReason(err.toString(), 'Bad response'),
          );
        }
      }, onError: (Object err) {
        _emit(ErrorEvent(err));
        onSocketFailure(err);
      });
    }).catchError((Object err) {
      // Defensive: any unhandled throw inside the start coroutine.
      attempt.fail(err);
    });

    return attempt.connectedFuture;
  }

  /// Inspects a connection error/close and decides whether to retry.
  ///
  /// Returns `true` to continue retrying (caller should loop), `false` if
  /// the disconnect should terminate the subscription cleanly. Re-throws
  /// any unrecoverable error.
  bool _shouldRetryConnectOrThrow(Object errOrCloseEvent) {
    if (errOrCloseEvent is LikeCloseEvent) {
      final fatal = isFatalInternalCloseCode(errOrCloseEvent.code) ||
          const <int>{
            // Mirrors the JS fatal list.
            4500, // CloseCode.internalServerError
            4005, // CloseCode.internalClientError
            4400, // CloseCode.badRequest
            4004, // CloseCode.badResponse
            4401, // CloseCode.unauthorized
            // CloseCode.forbidden — might grant access on retry
            4406, // CloseCode.subprotocolNotAcceptable
            // CloseCode.connectionInitialisationTimeout — might not time out on retry
            // CloseCode.connectionAcknowledgementTimeout — likewise
            4409, // CloseCode.subscriberAlreadyExists
            4429, // CloseCode.tooManyInitialisationRequests
            // 4499 Terminated — retryable on purpose
          }.contains(errOrCloseEvent.code);
      if (fatal) {
        // ignore: only_throw_errors
        throw errOrCloseEvent;
      }
    }

    if (_disposed) return false;

    if (errOrCloseEvent is LikeCloseEvent && errOrCloseEvent.code == 1000) {
      return _locks > 0;
    }

    if (retryAttempts == 0 || _retries >= retryAttempts) {
      // ignore: only_throw_errors
      throw errOrCloseEvent;
    }
    if (!shouldRetry(errOrCloseEvent)) {
      // ignore: only_throw_errors
      throw errOrCloseEvent;
    }

    _retrying = true;
    return true;
  }

  void _startNonLazyConnect() {
    _locks++;
    Future<void>(() async {
      while (true) {
        try {
          final connected = await _connect();
          // We never call connected.release(); we want to hold the lock
          // forever in non-lazy mode. The future will only complete via the
          // throwOnClose branch.
          await connected.waitForReleaseOrClose;
        } on Object catch (err) {
          try {
            if (!_shouldRetryConnectOrThrow(err)) return;
          } on Object catch (fatal) {
            onNonLazyError?.call(fatal);
            return;
          }
        }
      }
    });
  }

  Future<void>
      _runSubscription<TData extends Object?, TExtensions extends Object?>(
    String id,
    SubscribePayload payload,
    GraphqlSink<FormattedExecutionResult<TData, TExtensions>> sink,
    _SubscriptionState state,
  ) async {
    _locks++;
    while (true) {
      try {
        final connected = await _connect();
        if (state.done) {
          connected.release();
          return;
        }

        final unlisten = _listenForMessageId(id, (Message message) {
          switch (message) {
            case NextMessage(:final payload):
              final typed =
                  FormattedExecutionResult<TData, TExtensions>.fromJson(
                      payload.json);
              sink.next(typed);
            case ErrorMessage(:final payload):
              state.errored = true;
              state.done = true;
              sink.error(payload);
              state.releaser();
            case CompleteMessage():
              state.done = true;
              state.releaser();
            case _:
              break;
          }
        });

        connected.adapter.sendText(stringifyMessage(
          SubscribeMessage(id: id, payload: payload),
          replacer: jsonMessageReplacer,
        ));

        state.releaser = () {
          if (!state.done && connected.adapter.isOpen) {
            connected.adapter.sendText(stringifyMessage(
              CompleteMessage(id: id),
              replacer: jsonMessageReplacer,
            ));
          }
          _locks--;
          state.done = true;
          connected.release();
        };

        try {
          await connected.waitForReleaseOrClose;
        } finally {
          unlisten();
        }
        return;
      } on Object catch (err) {
        try {
          if (!_shouldRetryConnectOrThrow(err)) return;
        } on Object {
          rethrow;
        }
      }
    }
  }
}

class _Connected {
  _Connected({required this.adapter, required this.throwOnClose});

  final WebSocketAdapter adapter;
  final Future<void> throwOnClose;
}

class _Released {
  _Released({
    required this.adapter,
    required this.release,
    required this.waitForReleaseOrClose,
  });

  final WebSocketAdapter adapter;
  final void Function() release;
  final Future<void> waitForReleaseOrClose;
}

class _SubscriptionState {
  bool done = false;
  bool errored = false;
  void Function() releaser = () {};
}

/// Encapsulates the per-attempt lifecycle so the connect/post-connect
/// transitions are explicit: before `acknowledge()`, failures reject the
/// outer `connecting` promise; after, they reject `throwOnClose`.
class _ConnectionAttempt {
  final _connected = Completer<_Connected>();
  final _throwOnClose = Completer<void>();

  WebSocketAdapter? adapter;
  bool acknowledged = false;
  bool handled = false;
  StreamSubscription<String>? messagesSubscription;

  /// The connect-time closure's failure entry point — set by `_startConnect`
  /// so external paths (e.g. `terminate()`) can synthesise a failure without
  /// duplicating teardown logic.
  void Function(Object errOrEvent)? failureHandler;

  Future<_Connected> get connectedFuture => _connected.future;

  void completeConnected() {
    if (_connected.isCompleted) return;
    if (adapter == null) {
      _connected.completeError(StateError(
          'Internal error: adapter not set before completeConnected()'));
      return;
    }
    _connected.complete(
        _Connected(adapter: adapter!, throwOnClose: _throwOnClose.future));
  }

  /// Pre-ack failure path: reject the connecting promise.
  void failConnecting(Object err) {
    if (!_connected.isCompleted) {
      _connected.completeError(err);
    }
  }

  /// Generic failure entry point: routes to connecting or post-ack
  /// `throwOnClose` depending on whether ack was received.
  void fail(Object err) {
    if (!acknowledged) {
      failConnecting(err);
    } else if (!_throwOnClose.isCompleted) {
      _throwOnClose.completeError(err);
    }
    unawaited(messagesSubscription?.cancel());
    messagesSubscription = null;
  }
}

class _ControllerSink<T> implements GraphqlSink<T> {
  _ControllerSink(this._controller);

  final StreamController<T> _controller;

  @override
  void next(T value) {
    if (!_controller.isClosed) _controller.add(value);
  }

  @override
  void error(Object err) {
    if (_controller.isClosed) return;
    _controller.addError(err);
    unawaited(_controller.close());
  }

  @override
  void complete() {
    if (!_controller.isClosed) unawaited(_controller.close());
  }
}

extension on Duration {
  /// Whether this duration is finite (always true for `Duration`, but exposes
  /// a name parallel to JS's `isFinite` for readability at call sites).
  bool get isFinite => true;
}
