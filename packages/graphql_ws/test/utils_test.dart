/// Tests for close-code classification helpers in `src/utils.dart`.
///
/// `isFatalInternalCloseCode` is library-internal (not exported from
/// `graphql_ws.dart`), so it is imported via its `src/` path — the standard
/// approach for same-package tests.
@Tags(['utils'])
library;

import 'package:graphql_ws/src/utils.dart';
import 'package:test/test.dart';

void main() {
  group('isFatalInternalCloseCode', () {
    // The documented non-fatal (retryable) set. Keep in sync with the doc
    // comment on the function.
    const nonFatal = <int>{1000, 1001, 1002, 1005, 1006, 1012, 1013, 1014};

    test('classifies every documented non-fatal code as non-fatal', () {
      for (final code in nonFatal) {
        expect(
          isFatalInternalCloseCode(code),
          isFalse,
          reason: '$code is documented as non-fatal/retryable',
        );
      }
    });

    test('1002 (Protocol Error) is non-fatal', () {
      // Regression guard: Android emits 1002 on abrupt network disconnects,
      // not just true protocol violations. Treating it as fatal stopped the
      // client from retrying, killing live subscriptions on every wifi drop.
      expect(isFatalInternalCloseCode(1002), isFalse);
    });

    test('classifies other codes in the 1000-1999 range as fatal', () {
      // A representative sample of internal codes NOT in the non-fatal set.
      const fatal = <int>{1003, 1004, 1007, 1008, 1009, 1010, 1011, 1015, 1999};
      for (final code in fatal) {
        expect(
          isFatalInternalCloseCode(code),
          isTrue,
          reason: '$code is an internal code outside the non-fatal set',
        );
      }
    });

    test('treats range boundaries correctly', () {
      // 1000 is the bottom of the range but is explicitly non-fatal.
      expect(isFatalInternalCloseCode(1000), isFalse);
      // 1003 is the lowest fatal internal code (1000-1002 are non-fatal).
      expect(isFatalInternalCloseCode(1003), isTrue);
      // 1999 is the top of the fatal range; 2000 is just outside it.
      expect(isFatalInternalCloseCode(1999), isTrue);
      expect(isFatalInternalCloseCode(2000), isFalse);
    });

    test('classifies codes outside the 1000-1999 range as non-fatal', () {
      // Below the range, and application/library codes at/above 4000 — these
      // are not "internal" codes, so this predicate never flags them. (The
      // client applies a separate fatal list to 4xxx codes.)
      for (final code in <int>[0, 999, 2000, 3000, 4000, 4499, 4500]) {
        expect(
          isFatalInternalCloseCode(code),
          isFalse,
          reason: '$code is outside the internal 1000-1999 range',
        );
      }
    });
  });
}
