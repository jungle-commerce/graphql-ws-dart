# graphql_ws_flutter

Flutter integration for [`graphql_ws`](https://pub.dev/packages/graphql_ws).
Keeps a GraphQL over WebSocket connection alive across app lifecycle changes
and flaky mobile connectivity.

## Why

Mobile platforms silently freeze or kill WebSocket sockets when an app is
backgrounded. When the app resumes, the socket often looks open but is dead —
a "zombie" connection. Network handoffs (wifi ↔ cellular) cause the same
problem. This package detects those situations and drives
`Client.terminate()`, letting `graphql_ws`'s own retry machinery reconnect and
resubscribe — seamlessly, without surfacing errors to your widgets for
transient blips.

## Install

```yaml
dependencies:
  graphql_ws: ^0.1.0
  graphql_ws_flutter: ^0.1.0
```

## Usage

Wrap your app (or any subtree that owns the `Client`) in a
`GraphqlWsConnectionScope`:

```dart
import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';

final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  retryAttempts: 20, // generous — mobile networks are flaky
  retryWait: connectivityAwareRetryWait(), // park retries while offline
);

runApp(
  GraphqlWsConnectionScope(
    client: client,
    child: const MyApp(),
  ),
);
```

Or drive the manager yourself:

```dart
final manager = GraphqlWsConnectionManager(client: client)..attach();
// ...
manager.dispose();
```

## How it decides

On `resumed`, it measures how long the app was backgrounded:

- **brief** (< `trustWindow`, default 5s) — do nothing, the socket survived.
- **medium** — send a `ping` and start a watchdog; if no `pong` arrives within
  `verificationTimeout` (default 4s), `terminate()`.
- **long** (> `deadAfter`, default 3min) — `terminate()` immediately.

On a connectivity change to an online state (including wifi ↔ cellular
handoffs), it `terminate()`s so the client reconnects on the new interface.

## License

MIT — see [LICENSE](../../LICENSE).
