import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';

import 'utils/fake_connectivity_source.dart';
import 'utils/spy_client.dart';

void main() {
  testWidgets('renders its child', (tester) async {
    final connectivity = FakeConnectivitySource();
    addTearDown(connectivity.dispose);

    await tester.pumpWidget(GraphqlWsConnectionScope(
      client: SpyClient(),
      connectivitySource: connectivity,
      child: const Text('app', textDirection: TextDirection.ltr),
    ));

    expect(find.text('app'), findsOneWidget);
  });

  testWidgets('wires app lifecycle events through to the manager',
      (tester) async {
    final client = SpyClient();
    final connectivity = FakeConnectivitySource();
    addTearDown(connectivity.dispose);

    await tester.pumpWidget(GraphqlWsConnectionScope(
      client: client,
      connectivitySource: connectivity,
      terminateOnPause: true,
      child: const Text('app', textDirection: TextDirection.ltr),
    ));

    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.inactive);
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.hidden);
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.paused);

    expect(client.terminateCount, equals(1));
  });

  testWidgets('unmounting the scope detaches the manager', (tester) async {
    final client = SpyClient();
    final connectivity = FakeConnectivitySource();
    addTearDown(connectivity.dispose);

    await tester.pumpWidget(GraphqlWsConnectionScope(
      client: client,
      connectivitySource: connectivity,
      terminateOnPause: true,
      child: const Text('app', textDirection: TextDirection.ltr),
    ));

    // Replace the scope with a bare widget — its State.dispose runs.
    await tester.pumpWidget(
      const Text('replaced', textDirection: TextDirection.ltr),
    );

    // Lifecycle events must no longer reach the detached manager.
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.inactive);
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.hidden);
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.paused);

    expect(client.terminateCount, isZero);
    expect(find.text('replaced'), findsOneWidget);
  });
}
