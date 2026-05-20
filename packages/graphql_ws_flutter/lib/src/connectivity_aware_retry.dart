import 'dart:async';
import 'dart:math';

import 'package:graphql_ws/graphql_ws.dart';

import 'connectivity_source.dart';

/// Builds a [RetryWait] that parks reconnection attempts while the device is
/// offline instead of burning through the `retryAttempts` budget.
///
/// Pass the result as `retryWait` to `createClient`:
///
/// ```dart
/// createClient(
///   url: () => uri,
///   retryAttempts: 20,
///   retryWait: connectivityAwareRetryWait(),
/// );
/// ```
///
/// When a retry is requested while offline, the returned function does not
/// complete until connectivity is restored — so a long outage consumes a
/// single retry slot rather than rapidly exhausting the budget. Once online
/// (or if already online), [backoff] is applied before the function
/// completes. [backoff] defaults to a jittered exponential backoff capped at
/// 30 seconds.
RetryWait connectivityAwareRetryWait({
  ConnectivitySource? connectivitySource,
  RetryWait? backoff,
}) {
  final source = connectivitySource ?? ConnectivityPlusSource();
  final effectiveBackoff = backoff ?? _jitteredExponentialBackoff;
  return (int retries) async {
    if (!await source.isOnline()) {
      // Park until connectivity returns. `firstWhere` completes on the first
      // change event reporting an online state.
      await source.changes.firstWhere((online) => online);
    }
    await effectiveBackoff(retries);
  };
}

final Random _random = Random();

/// `2^retries` seconds (capped at 30s) plus 0.3–3.0s of jitter.
Future<void> _jitteredExponentialBackoff(int retries) {
  final baseSeconds = 1 << retries.clamp(0, 5);
  final cappedMs = (baseSeconds * 1000).clamp(0, 30000);
  final jitterMs = 300 + _random.nextInt(2700);
  return Future<void>.delayed(Duration(milliseconds: cappedMs + jitterMs));
}
