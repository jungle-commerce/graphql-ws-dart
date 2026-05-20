import 'dart:async';

import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';

/// A test [ConnectivitySource] whose connectivity state is driven manually.
class FakeConnectivitySource implements ConnectivitySource {
  FakeConnectivitySource({this.online = true});

  /// Current connectivity, as reported by [isOnline].
  bool online;

  final StreamController<bool> _controller = StreamController<bool>.broadcast();

  @override
  Stream<bool> get changes => _controller.stream;

  @override
  Future<bool> isOnline() async => online;

  /// Pushes a connectivity change event. Updates [online] and emits on
  /// [changes].
  void emit(bool value) {
    online = value;
    _controller.add(value);
  }

  /// Closes the underlying stream.
  Future<void> dispose() => _controller.close();
}
