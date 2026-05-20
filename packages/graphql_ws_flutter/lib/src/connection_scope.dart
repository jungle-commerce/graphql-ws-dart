import 'package:flutter/widgets.dart';
import 'package:graphql_ws/graphql_ws.dart';

import 'connection_manager.dart';
import 'connectivity_source.dart';

/// Wires a [GraphqlWsConnectionManager] into the widget tree.
///
/// Place it above any subtree that uses [client]; the manager is attached
/// in `initState` and disposed in `dispose`, so its lifetime follows this
/// widget. The [client] itself is **not** disposed — that stays the caller's
/// responsibility.
///
/// ```dart
/// runApp(
///   GraphqlWsConnectionScope(
///     client: client,
///     child: const MyApp(),
///   ),
/// );
/// ```
class GraphqlWsConnectionScope extends StatefulWidget {
  /// Creates a scope. See [GraphqlWsConnectionManager] for the meaning of
  /// each tuning parameter.
  const GraphqlWsConnectionScope({
    required this.client,
    required this.child,
    this.connectivitySource,
    this.trustWindow = const Duration(seconds: 5),
    this.deadAfter = const Duration(minutes: 3),
    this.verificationTimeout = const Duration(seconds: 4),
    this.terminateOnPause = false,
    this.onRecovery,
    super.key,
  });

  /// The client whose connection is kept alive.
  final Client client;

  /// The subtree below this scope.
  final Widget child;

  /// Connectivity provider; defaults to `ConnectivityPlusSource`.
  final ConnectivitySource? connectivitySource;

  /// See [GraphqlWsConnectionManager.trustWindow].
  final Duration trustWindow;

  /// See [GraphqlWsConnectionManager.deadAfter].
  final Duration deadAfter;

  /// See [GraphqlWsConnectionManager.verificationTimeout].
  final Duration verificationTimeout;

  /// See [GraphqlWsConnectionManager.terminateOnPause].
  final bool terminateOnPause;

  /// See [GraphqlWsConnectionManager.onRecovery].
  final void Function(RecoveryTrigger trigger)? onRecovery;

  @override
  State<GraphqlWsConnectionScope> createState() =>
      _GraphqlWsConnectionScopeState();
}

class _GraphqlWsConnectionScopeState extends State<GraphqlWsConnectionScope> {
  late final GraphqlWsConnectionManager _manager;

  @override
  void initState() {
    super.initState();
    _manager = GraphqlWsConnectionManager(
      client: widget.client,
      connectivitySource: widget.connectivitySource,
      trustWindow: widget.trustWindow,
      deadAfter: widget.deadAfter,
      verificationTimeout: widget.verificationTimeout,
      terminateOnPause: widget.terminateOnPause,
      onRecovery: widget.onRecovery,
    )..attach();
  }

  @override
  void dispose() {
    _manager.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.child;
}
