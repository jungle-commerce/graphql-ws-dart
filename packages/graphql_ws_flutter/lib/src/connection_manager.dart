import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:graphql_ws/graphql_ws.dart';

import 'connectivity_source.dart';

/// Why [GraphqlWsConnectionManager] triggered a reconnect. Reported through
/// the `onRecovery` callback — useful for logging and diagnostics.
enum RecoveryTrigger {
  /// The app resumed after being backgrounded longer than `deadAfter`; the
  /// socket was assumed dead without probing.
  staleAfterBackground,

  /// The app resumed, a `ping` was sent, and neither a `pong` nor a fresh
  /// connection arrived within `verificationTimeout` — the socket was frozen.
  failedLivenessProbe,

  /// Device connectivity changed to an online interface (e.g. a wifi-to-
  /// cellular handoff).
  connectivityChanged,

  /// The app was paused and `terminateOnPause` is enabled.
  appPaused,
}

/// Keeps a [Client]'s WebSocket connection healthy across app lifecycle and
/// connectivity changes.
///
/// It observes nothing destructive — on detecting a likely-dead ("zombie")
/// socket it calls [Client.terminate], delegating the actual reconnect and
/// resubscribe to the client's own retry machinery. It never disposes the
/// [client]; ownership of the client's lifetime stays with the caller.
///
/// Construct, then call [attach] to start observing and [dispose] to stop.
/// Most apps will use `GraphqlWsConnectionScope` instead of driving this
/// directly.
class GraphqlWsConnectionManager with WidgetsBindingObserver {
  /// Creates a manager for [client].
  ///
  /// - [connectivitySource]: connectivity provider; defaults to
  ///   `ConnectivityPlusSource`. Inject a fake in tests.
  /// - [trustWindow]: if the app was backgrounded for less than this, the
  ///   socket is trusted on resume and nothing happens.
  /// - [deadAfter]: if the app was backgrounded for longer than this, the
  ///   socket is assumed dead on resume and terminated without probing.
  /// - [verificationTimeout]: how long to wait for a `pong` (or a fresh
  ///   connection) after the resume liveness `ping` before declaring the
  ///   socket frozen.
  /// - [terminateOnPause]: when `true`, also terminate the connection as the
  ///   app is paused (a clean shutdown that tells the server immediately).
  ///   Defaults to `false` — by default the manager acts only on resume.
  /// - [onRecovery]: invoked whenever a reconnect is triggered. Diagnostic
  ///   only; `terminate()` is a no-op when there is no live connection, so a
  ///   trigger does not strictly guarantee a connection was torn down.
  GraphqlWsConnectionManager({
    required this.client,
    ConnectivitySource? connectivitySource,
    this.trustWindow = const Duration(seconds: 5),
    this.deadAfter = const Duration(minutes: 3),
    this.verificationTimeout = const Duration(seconds: 4),
    this.terminateOnPause = false,
    this.onRecovery,
  }) : _connectivitySource = connectivitySource ?? ConnectivityPlusSource();

  /// The client whose connection is being kept alive.
  final Client client;

  /// Resume-after-background durations shorter than this are ignored.
  final Duration trustWindow;

  /// Resume-after-background durations longer than this skip the ping probe
  /// and terminate immediately.
  final Duration deadAfter;

  /// How long to wait for a `pong` during the resume liveness probe.
  final Duration verificationTimeout;

  /// Whether to terminate the connection when the app is paused.
  final bool terminateOnPause;

  /// Invoked when the manager triggers a reconnect.
  final void Function(RecoveryTrigger trigger)? onRecovery;

  final ConnectivitySource _connectivitySource;

  StreamSubscription<bool>? _connectivitySub;
  Timer? _watchdog;
  void Function()? _verifyUnlisten;

  DateTime? _backgroundedAt;
  bool _verifying = false;
  bool _attached = false;

  /// Starts observing app lifecycle and connectivity. Idempotent.
  void attach() {
    if (_attached) return;
    _attached = true;
    WidgetsBinding.instance.addObserver(this);
    _connectivitySub =
        _connectivitySource.changes.listen(_onConnectivityChange);
  }

  /// Stops observing and releases resources. Does **not** dispose [client].
  /// Idempotent.
  void dispose() {
    if (!_attached) return;
    _attached = false;
    WidgetsBinding.instance.removeObserver(this);
    _verifyUnlisten?.call();
    _verifyUnlisten = null;
    _watchdog?.cancel();
    _watchdog = null;
    _verifying = false;
    unawaited(_connectivitySub?.cancel());
    _connectivitySub = null;
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (!_attached) return;
    switch (state) {
      case AppLifecycleState.resumed:
        _onResumed();
      case AppLifecycleState.hidden:
      case AppLifecycleState.paused:
        _backgroundedAt ??= DateTime.now();
        if (terminateOnPause && state == AppLifecycleState.paused) {
          _trigger(RecoveryTrigger.appPaused);
        }
      case AppLifecycleState.inactive:
      case AppLifecycleState.detached:
        break;
    }
  }

  void _onResumed() {
    final since = _backgroundedAt;
    _backgroundedAt = null;
    if (since == null) return;
    final elapsed = DateTime.now().difference(since);
    if (elapsed < trustWindow) return;
    if (elapsed > deadAfter) {
      _trigger(RecoveryTrigger.staleAfterBackground);
      return;
    }
    _verify();
  }

  /// Sends a `ping` and terminates unless the socket proves itself alive
  /// before the watchdog fires.
  ///
  /// Liveness is proven by *either* a `pong` *or* a fresh [ConnectedEvent] —
  /// the latter covers the case where the client was already reconnecting on
  /// its own, so the watchdog never tears down a healthy new connection.
  void _verify() {
    if (_verifying) return;
    _verifying = true;
    var alive = false;
    final offPong = client.on<PongEvent>((e) {
      if (e.received) alive = true;
    });
    final offConnected = client.on<ConnectedEvent>((_) => alive = true);
    _verifyUnlisten = () {
      offPong();
      offConnected();
    };
    client.ping();
    _watchdog = Timer(verificationTimeout, () {
      _verifyUnlisten?.call();
      _verifyUnlisten = null;
      _watchdog = null;
      _verifying = false;
      if (!alive) _trigger(RecoveryTrigger.failedLivenessProbe);
    });
  }

  void _onConnectivityChange(bool online) {
    // A connectivity event from `connectivity_plus` fires only on a real
    // interface change — reliable evidence that any live socket is now bound
    // to a stale interface. Terminate so the client reconnects on the new
    // one. (While disconnected this is a harmless no-op.)
    if (online) _trigger(RecoveryTrigger.connectivityChanged);
  }

  void _trigger(RecoveryTrigger trigger) {
    client.terminate();
    onRecovery?.call(trigger);
  }
}
