# graphql_ws_flutter example

A self-contained demo of [`graphql_ws_flutter`](../).

It starts an in-process `graphql-transport-ws` server (so there is nothing to
set up), connects to it with a resilient client, and shows a live counter
subscription, the connection status, and a log of every reconnection.

```sh
cd packages/graphql_ws_flutter/example
flutter run
```

Things to try:

- **Drop connection** — closes the socket server-side; watch the client
  reconnect and the counter resume.
- **Terminate** — calls `client.terminate()` directly.
- **Background the app** for a few seconds, then return — the
  `GraphqlWsConnectionManager` probes the socket and reconnects if needed.

The wiring that matters is in [`lib/main.dart`](lib/main.dart): a `createClient`
call using `connectivityAwareRetryWait`, wrapped in a
`GraphqlWsConnectionScope`.
