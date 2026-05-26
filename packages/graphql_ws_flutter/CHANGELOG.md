## 0.1.0

- Initial release. Flutter integration for
  [`graphql_ws`](https://pub.dev/packages/graphql_ws):
  - `GraphqlWsConnectionManager` — observes app lifecycle and connectivity,
    verifies the socket on resume (ping + watchdog), and drives
    `Client.terminate()` when it detects a frozen or dead connection.
  - `GraphqlWsConnectionScope` — a widget that wires the manager into the
    widget tree.
  - `connectivityAwareRetryWait` — a `RetryWait` that parks reconnection
    attempts while the device is offline instead of burning the retry budget.
