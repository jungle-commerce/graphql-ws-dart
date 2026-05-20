# graphql_ws_flutter

Flutter integration for [`graphql_ws`](https://pub.dev/packages/graphql_ws).
Keeps a GraphQL over WebSocket connection alive across app lifecycle changes
and flaky mobile connectivity — so reconnection is seamless and your widgets
never see an error for a transient blip.

## Install

```yaml
dependencies:
  graphql_ws: ^0.1.0
  graphql_ws_flutter: ^0.1.0
```

## Quick start

```dart
import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';

final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),
  retryAttempts: 20,                          // a generous budget
  retryWait: connectivityAwareRetryWait(),    // park retries while offline
);

runApp(
  GraphqlWsConnectionScope(                   // reconnect after backgrounding
    client: client,
    child: const MyApp(),
  ),
);
```

Those three highlighted choices are the whole story. The rest of this README
explains why each one matters.

## Handling retries & flaky connectivity

Mobile connections drop constantly — tunnels, elevators, lifts, dead zones,
wifi↔cellular handoffs, the OS suspending a backgrounded app. Here is exactly
how this library handles each case, and what you need to configure.

### What `graphql_ws` already does for you

`graphql_ws` reconnects automatically. On a disconnect it retries (up to
`retryAttempts`, default `5`) with exponential backoff and **resubscribes
every active subscription**. Your `Stream` / `Sink` does **not** receive an
error while this happens — it pauses, then resumes. An error only reaches
subscribers once the retry budget is fully exhausted. The budget resets to
zero on every successful connection, so a client that recovers gets a fresh
budget for the next blip.

That covers ordinary disconnects — a server restart, a brief network drop.
Two mobile-specific problems remain, and that is what this package adds.

### Problem 1 — the socket is dead but the client doesn't know it

When the OS suspends a backgrounded app, it freezes or silently kills the
WebSocket. The socket still reports itself "open", no close event fires, so
the retry machinery is never triggered. On resume the app looks connected but
no data flows — a **zombie socket**.

**`GraphqlWsConnectionScope` fixes this.** On resume it actively probes the
socket (sends a `ping`, waits for a `pong`) and, if the socket is dead, calls
`client.terminate()` — which kicks the normal retry-and-resubscribe path. It
also watches connectivity, so a wifi↔cellular handoff reconnects too.

### Problem 2 — retries burn out during an outage

While the device is offline, every reconnect attempt fails *immediately*.
With the default backoff, a budget of 5 is spent in about a minute — then
your subscriptions error out, even though the user just walked into a tunnel
for two minutes.

**`connectivityAwareRetryWait` fixes this.** While the device is offline it
*parks* the retry — the attempt simply waits for connectivity to return
instead of failing. A 20-minute outage consumes a *single* retry slot, not
twenty. The instant the network is back, it reconnects and resubscribes.

### Putting it together

```dart
final client = createClient(
  url: () => Uri.parse('wss://example.com/graphql'),

  // A budget for genuine, repeated failures. Because connectivityAwareRetryWait
  // parks (rather than spends) attempts while offline, this is no longer a
  // budget for *time* offline — so it can be generous.
  retryAttempts: 20,

  // Park retries while offline instead of burning the budget.
  retryWait: connectivityAwareRetryWait(),
);

runApp(GraphqlWsConnectionScope(client: client, child: const MyApp()));
```

| Scenario | Handled by |
|---|---|
| Server restart, brief network drop | `graphql_ws` built-in retry (`retryAttempts` + backoff) |
| App backgrounded → socket frozen ("zombie") | `GraphqlWsConnectionScope` — resume probe |
| Wifi ↔ cellular handoff | `GraphqlWsConnectionScope` — connectivity observer |
| Extended offline stretch (tunnel, airplane mode) | `connectivityAwareRetryWait` — parks retries |

### What your UI sees

Through all of the above your subscription **streams do not emit an error** —
they pause and resume. A `StreamBuilder` keeps showing its last value while
reconnecting; you usually need to do nothing. To show a "reconnecting…"
indicator, listen to lifecycle events:

```dart
client.on<ClientEvent>((event) {
  switch (event) {
    case ConnectingEvent(isRetry: true): // show "reconnecting…"
    case ConnectedEvent():               // hide it
    case _:
  }
});
```

A subscription error surfaces **only** when `retryAttempts` is genuinely
exhausted — a sustained failure that is not merely being offline (e.g. a
server that keeps rejecting the connection). Treat that as a real "give up"
signal: show an error, offer a manual retry.

## How resume detection works

On resume, `GraphqlWsConnectionManager` measures how long the app was
backgrounded:

- **brief** (< `trustWindow`, default 5s) — do nothing, the socket survived.
- **medium** — send a `ping` and start a watchdog; if no `pong` (nor a fresh
  connection) arrives within `verificationTimeout` (default 4s),
  `terminate()`.
- **long** (> `deadAfter`, default 3min) — `terminate()` immediately, the
  socket is certainly dead.

## Tuning

Every knob is optional. Pass them to `GraphqlWsConnectionScope` (or
`GraphqlWsConnectionManager`):

| Option | Default | Purpose |
|---|---|---|
| `trustWindow` | 5s | Backgrounded shorter than this → trust the socket. |
| `deadAfter` | 3min | Backgrounded longer than this → terminate without probing. |
| `verificationTimeout` | 4s | How long to wait for a `pong` during the resume probe. |
| `terminateOnPause` | `false` | Also terminate when the app is paused (clean shutdown; costs a reconnect on every foreground). |
| `onRecovery` | — | Diagnostic callback — fires with a `RecoveryTrigger` whenever a reconnect is triggered. |
| `connectivitySource` | `ConnectivityPlusSource` | Connectivity provider; override to inject a fake in tests. |

## Driving it yourself

`GraphqlWsConnectionScope` is a thin widget over `GraphqlWsConnectionManager`.
If you do not have a convenient widget tree, use the manager directly:

```dart
final manager = GraphqlWsConnectionManager(client: client)..attach();
// ...later:
manager.dispose(); // does NOT dispose the client
```

## Example

See [`example/`](example) for a self-contained, runnable demo — it starts an
in-process server, shows a live counter subscription, and lets you drop the
connection and watch it recover.

## License

MIT — see [LICENSE](../../LICENSE).
