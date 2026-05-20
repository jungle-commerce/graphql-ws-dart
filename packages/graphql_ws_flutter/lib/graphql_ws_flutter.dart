/// Flutter integration for [`graphql_ws`](https://pub.dev/packages/graphql_ws).
///
/// Keeps a GraphQL over WebSocket connection alive across app lifecycle
/// changes and flaky mobile connectivity. Mobile platforms silently freeze
/// or kill WebSocket sockets when an app is backgrounded; this package
/// detects that and drives `Client.terminate()` so the client's own retry
/// machinery reconnects and resubscribes seamlessly.
library;

export 'src/connection_manager.dart';
export 'src/connection_scope.dart';
export 'src/connectivity_aware_retry.dart';
export 'src/connectivity_source.dart';
