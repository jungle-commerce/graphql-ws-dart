import 'dart:async';

/// Promise-with-resolvers helper, used liberally in tests for explicit
/// rendezvous between async events.
class Deferred<T> {
  Deferred() : _completer = Completer<T>();

  final Completer<T> _completer;

  Future<T> get future => _completer.future;

  bool get isCompleted => _completer.isCompleted;

  void resolve([T? value]) {
    if (_completer.isCompleted) return;
    _completer.complete(value as T);
  }

  void reject(Object error, [StackTrace? stackTrace]) {
    if (_completer.isCompleted) return;
    _completer.completeError(error, stackTrace);
  }
}
