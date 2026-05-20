import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';

import 'utils/fake_connectivity_source.dart';

void main() {
  group('connectivityAwareRetryWait', () {
    test('completes promptly when already online', () async {
      final source = FakeConnectivitySource(online: true);
      addTearDown(source.dispose);

      final retryWait = connectivityAwareRetryWait(
        connectivitySource: source,
        backoff: (_) async {},
      );

      await retryWait(0).timeout(const Duration(seconds: 1));
    });

    test('parks while offline, then completes once connectivity returns',
        () async {
      final source = FakeConnectivitySource(online: false);
      addTearDown(source.dispose);

      final retryWait = connectivityAwareRetryWait(
        connectivitySource: source,
        backoff: (_) async {},
      );

      var completed = false;
      unawaited(retryWait(0).then((_) => completed = true));

      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(completed, isFalse, reason: 'should park while offline');

      source.emit(true);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(completed, isTrue, reason: 'should un-park when back online');
    });

    test('ignores offline events while parking, waits for an online one',
        () async {
      final source = FakeConnectivitySource(online: false);
      addTearDown(source.dispose);

      final retryWait = connectivityAwareRetryWait(
        connectivitySource: source,
        backoff: (_) async {},
      );

      var completed = false;
      unawaited(retryWait(1).then((_) => completed = true));

      source.emit(false); // still offline — must not un-park
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(completed, isFalse);

      source.emit(true);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(completed, isTrue);
    });

    test('applies the supplied backoff after connectivity is confirmed',
        () async {
      final source = FakeConnectivitySource(online: true);
      addTearDown(source.dispose);

      var backoffCalledWith = -1;
      final retryWait = connectivityAwareRetryWait(
        connectivitySource: source,
        backoff: (retries) async => backoffCalledWith = retries,
      );

      await retryWait(3);
      expect(backoffCalledWith, equals(3));
    });
  });
}
