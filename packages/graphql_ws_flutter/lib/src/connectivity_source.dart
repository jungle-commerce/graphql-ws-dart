import 'package:connectivity_plus/connectivity_plus.dart';

/// A source of device connectivity information.
///
/// Abstracted behind an interface so tests can inject a fake instead of
/// driving the real platform plugin.
abstract interface class ConnectivitySource {
  /// Emits whenever connectivity changes; the value is whether the device is
  /// now online.
  ///
  /// Values may repeat — a wifi-to-cellular handoff emits `true` again — so
  /// consumers that care about handoffs must react to every event, not just
  /// to `false`-to-`true` transitions.
  Stream<bool> get changes;

  /// Best-effort current connectivity. `true` when the device has a network
  /// interface other than "none".
  Future<bool> isOnline();
}

/// A [ConnectivitySource] backed by `package:connectivity_plus`.
class ConnectivityPlusSource implements ConnectivitySource {
  /// Creates a source. Pass a [Connectivity] instance to override the default
  /// singleton (useful in tests, though most tests will use a fake instead).
  ConnectivityPlusSource([Connectivity? connectivity])
      : _connectivity = connectivity ?? Connectivity();

  final Connectivity _connectivity;

  @override
  Stream<bool> get changes =>
      _connectivity.onConnectivityChanged.map(_isOnline);

  @override
  Future<bool> isOnline() async =>
      _isOnline(await _connectivity.checkConnectivity());

  /// The device is online if any reported interface is not "none".
  static bool _isOnline(List<ConnectivityResult> results) =>
      results.any((r) => r != ConnectivityResult.none);
}
