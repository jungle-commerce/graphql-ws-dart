/// Test helper mirroring the JS `tsubscribe` utility — wraps `Client.subscribe`
/// in queue+`waitForX` ergonomics so tests can assert per-result delivery.
library;

import 'dart:async';

import 'package:graphql_ws/graphql_ws.dart';

import 'deferred.dart';

const _defaultTimeout = Duration(seconds: 1);

class TSubscribe<TData extends Object?, TExtensions extends Object?> {
  TSubscribe._(this._dispose);

  static TSubscribe<TData, TExtensions>
      start<TData extends Object?, TExtensions extends Object?>(
    Client client,
    SubscribePayload payload,
  ) {
    final results = <FormattedExecutionResult<TData, TExtensions>>[];
    final pendingNext = <Deferred<void>>[];
    Object? error;
    bool completed = false;
    final completeWaiters = <Deferred<void>>[];
    final errorWaiters = <Deferred<void>>[];

    final dispose = client.subscribe<TData, TExtensions>(
      payload,
      _CallbackSink<FormattedExecutionResult<TData, TExtensions>>(
        onNext: (value) {
          results.add(value);
          if (pendingNext.isNotEmpty) {
            pendingNext.removeAt(0).resolve();
          }
        },
        onError: (err) {
          error = err;
          for (final w in errorWaiters) {
            w.resolve();
          }
          errorWaiters.clear();
          // Cancel any pending next-waiters; the subscription is now dead.
          for (final w in pendingNext) {
            w.reject(StateError('subscription errored before next'));
          }
          pendingNext.clear();
        },
        onComplete: () {
          completed = true;
          for (final w in completeWaiters) {
            w.resolve();
          }
          completeWaiters.clear();
          for (final w in pendingNext) {
            w.reject(StateError('subscription completed before next'));
          }
          pendingNext.clear();
        },
      ),
    );

    final sub = TSubscribe<TData, TExtensions>._(dispose);
    sub._results = results;
    sub._pendingNext = pendingNext;
    sub._errorRef = () => error;
    sub._completedRef = () => completed;
    sub._completeWaiters = completeWaiters;
    sub._errorWaiters = errorWaiters;
    return sub;
  }

  final void Function() _dispose;
  late final List<FormattedExecutionResult<TData, TExtensions>> _results;
  late final List<Deferred<void>> _pendingNext;
  late final Object? Function() _errorRef;
  late final bool Function() _completedRef;
  late final List<Deferred<void>> _completeWaiters;
  late final List<Deferred<void>> _errorWaiters;

  /// Dispose the underlying subscription.
  void dispose() => _dispose();

  /// Wait for the next emitted result. If the timeout fires before a result
  /// arrives, returns `null` (mirrors the JS `expire` parameter).
  Future<FormattedExecutionResult<TData, TExtensions>?> waitForNext({
    Duration timeout = _defaultTimeout,
  }) async {
    if (_results.isNotEmpty) {
      return _results.removeAt(0);
    }
    final d = Deferred<void>();
    _pendingNext.add(d);
    try {
      await d.future.timeout(timeout, onTimeout: () {
        _pendingNext.remove(d);
        return;
      });
    } on Object {
      return null;
    }
    if (_results.isEmpty) return null;
    return _results.removeAt(0);
  }

  /// Wait for an error to be delivered to the sink. Returns the error or
  /// `null` on timeout.
  Future<Object?> waitForError({Duration timeout = _defaultTimeout}) async {
    if (_errorRef() != null) return _errorRef();
    final d = Deferred<void>();
    _errorWaiters.add(d);
    try {
      await d.future.timeout(timeout, onTimeout: () {
        _errorWaiters.remove(d);
        return;
      });
    } on Object {
      return null;
    }
    return _errorRef();
  }

  /// Wait for the sink to complete. Returns `true` on completion, `false`
  /// on timeout.
  Future<bool> waitForComplete({Duration timeout = _defaultTimeout}) async {
    if (_completedRef()) return true;
    final d = Deferred<void>();
    _completeWaiters.add(d);
    try {
      await d.future.timeout(timeout, onTimeout: () {
        _completeWaiters.remove(d);
        return;
      });
    } on Object {
      return false;
    }
    return _completedRef();
  }
}

class _CallbackSink<T> implements GraphqlSink<T> {
  _CallbackSink({
    required this.onNext,
    required this.onError,
    required this.onComplete,
  });

  final void Function(T value) onNext;
  final void Function(Object error) onError;
  final void Function() onComplete;

  @override
  void next(T value) => onNext(value);

  @override
  void error(Object err) => onError(err);

  @override
  void complete() => onComplete();
}
