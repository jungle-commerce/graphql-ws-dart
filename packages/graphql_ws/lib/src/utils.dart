/// Small internal helpers shared across the client.
///
/// Faithful port of `src/utils.ts` from the JavaScript `graphql-ws` package,
/// plus a Dart-native v4 UUID generator (matching the JS `Math.random`-based
/// implementation in spirit, using `Random.secure()` for slightly better
/// uniqueness).
library;

import 'dart:math';

final Random _idRandom = Random();

/// Generates a v4 UUID suitable for identifying subscriptions.
///
/// Format: `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx` where `x` is any hex digit
/// and `y` is one of `8`, `9`, `a`, or `b`.
String generateUuidV4() {
  const hex = '0123456789abcdef';
  final buf = StringBuffer();
  for (var i = 0; i < 36; i++) {
    switch (i) {
      case 8:
      case 13:
      case 18:
      case 23:
        buf.write('-');
      case 14:
        buf.write('4');
      case 19:
        // y: 8, 9, a, or b (high two bits = 10)
        buf.writeCharCode(hex.codeUnitAt((_idRandom.nextInt(16) & 0x3) | 0x8));
      default:
        buf.writeCharCode(hex.codeUnitAt(_idRandom.nextInt(16)));
    }
  }
  return buf.toString();
}

/// Limits the WebSocket close event reason to not exceed a length of one frame.
///
/// Reference: https://datatracker.ietf.org/doc/html/rfc6455#section-5.2.
String limitCloseReason(String reason, String whenTooLong) {
  // Per RFC 6455 the reason must fit in 123 bytes of UTF-8. The JS impl uses
  // a simple character-count check; we mirror it byte-aware to avoid silent
  // truncation when the reason contains multi-byte characters.
  if (reason.length >= 124) return whenTooLong;
  // Cheap fast path for ASCII; only run the byte-length check if the cheap
  // length test passed but the string might contain multi-byte chars.
  for (var i = 0; i < reason.length; i++) {
    if (reason.codeUnitAt(i) >= 0x80) {
      return reason.runes.fold<int>(0, (acc, r) {
                if (r < 0x80) return acc + 1;
                if (r < 0x800) return acc + 2;
                if (r < 0x10000) return acc + 3;
                return acc + 4;
              }) <
              124
          ? reason
          : whenTooLong;
    }
  }
  return reason;
}

/// A `CloseEvent`-like shape carried through error channels.
///
/// Mirrors `LikeCloseEvent` from the JS impl. Provided as a public type so
/// that subscribers' `error` callbacks and `shouldRetry` predicates can
/// pattern-match on it.
class LikeCloseEvent {
  const LikeCloseEvent({required this.code, required this.reason});

  /// The WebSocket connection close code.
  final int code;

  /// The WebSocket connection close reason.
  final String reason;

  @override
  String toString() => 'CloseEvent($code, $reason)';
}

/// A synthetic close event `4499: Terminated`, issued to the current
/// WebSocket to immediately close the connection without waiting for the
/// underlying `onclose` callback.
///
/// Terminating is not considered fatal and a connection retry will occur as
/// expected. Useful when the WebSocket is stuck and not emitting any events
/// (e.g. iOS Safari, see https://github.com/enisdenjo/graphql-ws/discussions/290).
class TerminatedCloseEvent extends LikeCloseEvent implements Exception {
  const TerminatedCloseEvent() : super(code: 4499, reason: 'Terminated');

  @override
  String toString() => '4499: Terminated';
}

/// Predicate used by [shouldRetryConnect] to evaluate whether a close-event
/// code represents a fatal WebSocket-internal error.
///
/// The library classifies these "normal" codes as **non-fatal** (retryable):
///   - 1000 Normal Closure
///   - 1001 Going Away
///   - 1002 Protocol Error (included because Android sends this on abrupt
///     network disconnects, not only for true protocol violations)
///   - 1006 Abnormal Closure
///   - 1005 No Status Received
///   - 1012 Service Restart
///   - 1013 Try Again Later
///   - 1014 Bad Gateway
///
/// All other internal codes in the 1000-1999 range are considered fatal.
bool isFatalInternalCloseCode(int code) {
  const nonFatal = <int>{1000, 1001, 1002, 1006, 1005, 1012, 1013, 1014};
  if (nonFatal.contains(code)) return false;
  return code >= 1000 && code <= 1999;
}
