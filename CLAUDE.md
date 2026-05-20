# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A monorepo housing a Dart port of [`graphql-ws`](https://github.com/enisdenjo/graphql-ws) (client only) and its companion packages. Implements the [GraphQL over WebSocket Protocol](https://github.com/graphql/graphql-over-http/blob/main/rfcs/GraphQLOverWebSocket.md) (`graphql-transport-ws` sub-protocol). The JS reference clone lives at `~/Projects/graphql-ws/` — read it first when porting new behavior or debugging protocol-level discrepancies.

## Repo layout

This is a [pub workspace](https://dart.dev/tools/pub/workspaces) (Dart 3.5+). The root `pubspec.yaml` lists workspace members; each package has `resolution: workspace` and shares one resolved set of deps.

```
graphql-ws-dart/
├── pubspec.yaml                                    workspace root (publish_to: none)
├── packages/
│   ├── graphql_ws/                                 zero-dep core client
│   │   ├── lib/
│   │   ├── test/
│   │   └── example/main.dart
│   ├── graphql_ws_test_server/                     shelf-backed graphql-transport-ws server for integration tests; not published
│   │   └── lib/graphql_ws_test_server.dart
│   └── graphql_ws_web_socket_channel_connector/    web_socket_channel-backed WebSocketConnector
│       └── lib/graphql_ws_web_socket_channel_connector.dart
└── LICENSE                                          shared across all packages
```

## Hard constraints

- **Zero external dependencies in `packages/graphql_ws/lib/`.** Its `pubspec.yaml` has no `dependencies:` section. Use `Object?` and explicit casts at JSON boundaries instead of pulling in helper packages. Test-only deps (`test`, `lints`) live in `dev_dependencies`. This constraint applies ONLY to `graphql_ws` — companion packages like `graphql_ws_web_socket_channel_connector` can depend on whatever they bridge.
- **No `dynamic`.** Use `Object?` everywhere a JSON-ish value crosses a boundary. The analyzer is configured with `strict-casts`/`strict-inference`/`strict-raw-types`, so `dynamic` will surface as a warning. Same rule for every package in this repo.
- **1:1 with the JS client for protocol semantics.** When in doubt about behavior (retry classification, lazy-close debouncing, ack-timeout, terminate, etc.), read `~/Projects/graphql-ws/src/client.ts` and mirror it. Tests in `~/Projects/graphql-ws/tests/client.test.ts` are the contract.
- **Don't add a `connector` default to `graphql_ws` that imports `dart:html` or `package:web_socket_channel`.** The default uses `dart:io.WebSocket` and that is intentional. Flutter-web users add the `graphql_ws_web_socket_channel_connector` companion package.

## Common commands

Run from the repo root — `pub` resolves the whole workspace at once.

```sh
dart pub get                                            # resolve deps across the workspace
dart analyze                                            # analyze every package; 0 issues expected
dart test packages/graphql_ws                           # run one package's tests
dart test packages/graphql_ws/test/client_test.dart     # one file
dart test -N 'lazy should disconnect after lazyClose'   # one case by name substring (run inside a package dir)
```

For per-package work, `cd` into the package directory and run `dart test`/`dart analyze` directly — that's what most of the existing notes assume.

There is no build step.

## Architecture

### `packages/graphql_ws` — core client

Layering (top of `lib/`):

```
graphql_ws.dart           public re-exports
  └── src/
      ├── common.dart           protocol types, validation, parse/stringify
      ├── events.dart           sealed ClientEvent hierarchy
      ├── utils.dart            UUID v4, LikeCloseEvent, isFatalInternalCloseCode
      ├── websocket_adapter.dart  WebSocketAdapter interface + dart:io default
      └── client.dart           Client + createClient state machine
```

`graphql_ws.dart` re-exports the user-facing API; internal helpers (e.g. `_GraphqlWsClient`, `_ConnectionAttempt`) stay private.

#### The state machine ([packages/graphql_ws/lib/src/client.dart](packages/graphql_ws/lib/src/client.dart))

This is where almost all the subtlety lives. The shape mirrors the JS reference closely; if you're tempted to "simplify", read the corresponding JS first.

- **`_connecting: Future<_Connected>?`** — the single in-flight connection promise. Shared by all subscribers. Cleared when the connection fails (in `onSocketFailure`).
- **`_activeAdapter` / `_activeAttempt`** — Dart-specific additions for clean dispose-mid-handshake. Set early in `_startConnect` so `dispose()` and `terminate()` can fail an attempt that's still inside `await connector(...)`. JS doesn't need these because Promise chains there terminate differently — be careful not to "remove these, they look redundant".
- **`_locks: int`** — count of live subscribers + (for non-lazy mode) a permanent +1 holding the connection open. The lazy-close `.then` callback in `_connect` checks `_locks == 0` to decide whether to actually tear down. **Microtask ordering matters**: `_locks++` runs *synchronously* inside `_runSubscription`, so a subscribe immediately following a dispose will keep the connection alive — this is the JS test "concurrency" group's expected semantics.
- **`_ConnectionAttempt`** encapsulates a single connect cycle. Two completers:
  - `_connected` — resolved on `ConnectionAck` or rejected if the socket fails before ack.
  - `_throwOnClose` — rejected when the socket fails after ack.
  Plus a `handled` flag so terminate() + adapter.done don't double-fire teardown, and a `failureHandler` closure exposed so `terminate()` can synthesise a `TerminatedCloseEvent` without duplicating onSocketFailure's logic.
- **Retry classification** in `_shouldRetryConnectOrThrow` — the fatal-codes set is taken from the JS impl; preserve it verbatim. Retries reset to zero only when a connection is *acknowledged*, not merely established.

#### `WebSocketAdapter` ([packages/graphql_ws/lib/src/websocket_adapter.dart](packages/graphql_ws/lib/src/websocket_adapter.dart))

Transport injection point. `DartIoWebSocketAdapter` wraps `dart:io.WebSocket` and **tracks locally-initiated close codes** — `dart:io` exposes `closeCode` reflecting only the *remote* side, so without this tracking, tests asserting `code == 4005` (locally initiated) would see 1005 instead. Don't remove the `_localCloseCode`/`_localCloseInitiated` plumbing.

#### Events ([packages/graphql_ws/lib/src/events.dart](packages/graphql_ws/lib/src/events.dart))

Sealed `ClientEvent` hierarchy. Consumers can either narrow via the type parameter (`client.on<ConnectedEvent>(...)`) or pattern-match exhaustively (`switch (event) { case ConnectedEvent(): ... }`). The wrapping in `_GraphqlWsClient.on` is an `is E` filter — types are erased at runtime, so the filter does the discrimination.

#### Test harness ([packages/graphql_ws/test/utils/](packages/graphql_ws/test/utils/))

- **`tserver.dart`** — scriptable WS server built on `dart:io.HttpServer`. **Not** a graphql-ws server; tests drive `ConnectionAck`, `Next`, `Error`, `Complete` frames manually. The JS reference uses a full GraphQL server for tests; we deliberately don't, so subscription-protocol behavior can be asserted without standing up a schema.
- **`tsub.dart`** — `TSubscribe.start<TData, TExtensions>(client, payload)` mirrors the JS `tsubscribe(client, payload)` helper. Returns waitable `waitForNext`/`waitForError`/`waitForComplete` futures.
- **`deferred.dart`** — `Completer` wrapper for explicit rendezvous in tests.

#### Tests cover

Two layers:

**Unit suite** (no real GraphQL execution; tserver mocks frames):
- `test/common_test.dart` — 73 cases, full port of upstream `common.test.ts` (every `it.each` row).
- `test/client_test.dart` — 54 cases across top-level + 7 `group`s (ping/pong, query op, subscription op, concurrency, lazy, reconnecting, events, stream, robustness). Substantially mirrors `client.test.ts`.
- `test/smoke_test.dart` — 4 wiring sanity checks.

One case is intentionally `skip:`ped with a tracking comment (listener throwing inside `ConnectedEvent` — semantics differ slightly).

**Integration suite** (real shelf-backed server, tagged `integration`):
- `test/integration_test.dart` — 12 cases against `graphql_ws_test_server` exercising the default `DartIoWebSocketAdapter` end-to-end: query/Complete, subscription stream, server-emitted error, keep-alive ping/pong, connectionParams round-trip, retry-after-server-disconnect, concurrent subscriptions sharing one socket, lazy connect/disconnect lifecycle, connectionAckWaitTimeout (4504), fatal close code (no retry), non-lazy + onNonLazyError, retry exhaustion.
- Mirror suite in `packages/graphql_ws_web_socket_channel_connector/test/integration_test.dart` — 5 cases against the same server but with `webSocketChannelConnector`. Focused on adapter-shape behaviors (Next/Complete, cancellation, clean close, error frames) rather than re-testing client behavior already covered by the core integration suite.

Total in `graphql_ws`: 131 unit + 12 integration = 143 + 1 skipped. Plus 5 integration in `graphql_ws_web_socket_channel_connector`. Grand total: 148 active.

### `packages/graphql_ws_web_socket_channel_connector` — web/cross-platform adapter

Single-file package. Exposes `webSocketChannelConnector` — a `WebSocketConnector` backed by `package:web_socket_channel`. Tracks locally-initiated close codes for the same reason `DartIoWebSocketAdapter` does. Lives in its own package so the `graphql_ws` core stays zero-dep.

### `packages/graphql_ws_test_server` — integration-test backend

`publish_to: none`. Minimal `graphql-transport-ws` compliant server built on `shelf` + `shelf_web_socket`. Operations are registered by `operationName`, dispatched to a handler returning `Stream<Map<String, Object?>>`. Stream values become `next` messages, stream errors become `error` messages, stream-done becomes `complete`. Reads `SERVER_HOST` env var (defaults to `localhost`); pass `host: '0.0.0.0'` to expose on the network — relevant for emulators / Firebase Test Lab in the future. Deliberately does NOT depend on `graphql_ws`, so server bugs can't paper over client bugs.

## Gotchas

- **`expect(() => future, throwsA(...))` does NOT await.** Use `await expectLater(...)`. The "no client appears" / "no message arrives" patterns in `client_test.dart` rely on this.
- **`dart:io.WebSocket.close(code)` with codes outside 1000-4999 throws `ArgumentError`.** The adapter catches and falls back to `socket.close()`. If you add new close codes, keep this guard.
- **`stringifyMessage` re-validates** via `validateMessage(msg.toJson())` — this is intentional 1:1 with JS. Don't "optimise" it away.
- **`limitCloseReason` is byte-aware** for multi-byte chars. The threshold is 123 bytes (RFC 6455).
- **Disposing while in `await connector(uri, ...)` is a real race.** `_startConnect` sets `_activeAttempt` *before* the async body runs, and the body re-checks `_disposed` after the connector resolves. Don't move that ordering.

## When porting new tests / behaviors from upstream

1. Read the JS test in `~/Projects/graphql-ws/tests/client.test.ts`.
2. Map server-side helpers: JS's `startTServer({onConnect, connectionInitWaitTimeout, ...})` runs a real GraphQL server with auto-ack. Our `TServer.start()` does NOT auto-ack — script the handshake explicitly via `server.waitForConnectAndAck()` or by hand.
3. JS `server.pong(key)` emits a result on the `ping(key)` subscription. In Dart, send a `NextMessage` with the subscription id directly via `tc.sendMessage(NextMessage(id: sm.id, payload: ...))`.
4. JS uses `webSocketImpl` injection; we use `connector`. If a test relies on injecting a custom WebSocket implementation, write a `WebSocketAdapter` instead.
5. The JS `replacer` option semantics (called for every key) don't map cleanly to `dart:convert.jsonEncode`'s `toEncodable` (called only for non-encodable values). The `jsonMessageReplacer` parameter exists but is documented as different; do not advertise 1:1 parity with the JS replacer.
