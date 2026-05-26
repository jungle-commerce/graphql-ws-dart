import 'dart:async';

import 'package:flutter/material.dart';
import 'package:graphql_ws/graphql_ws.dart';
import 'package:graphql_ws_flutter/graphql_ws_flutter.dart';
import 'package:graphql_ws_test_server/graphql_ws_test_server.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // A real app points `url` at its own backend. To keep this demo
  // self-contained and runnable with zero setup, we start an in-process
  // graphql-transport-ws server and connect to it over localhost.
  final server = await GraphqlWsTestServer.start();
  final uptime = Stopwatch()..start();
  server.register('counter', (_) async* {
    // Monotonic so the value keeps climbing across reconnects — a reconnect
    // looks seamless rather than resetting the counter.
    while (true) {
      yield {
        'data': {'count': uptime.elapsed.inSeconds},
      };
      await Future<void>.delayed(const Duration(seconds: 1));
    }
  });

  // The two choices that make a connection survive flaky mobile networks:
  //  - a generous retry budget, and
  //  - connectivityAwareRetryWait, which parks retries while the device is
  //    offline instead of burning the budget on doomed attempts.
  final client = createClient(
    url: () => server.uri,
    retryAttempts: 20,
    retryWait: connectivityAwareRetryWait(),
  );

  runApp(DemoApp(server: server, client: client));
}

enum _ConnState {
  connecting('Connecting…', Colors.orange),
  connected('Connected', Colors.green),
  reconnecting('Reconnecting…', Colors.orange),
  disconnected('Disconnected', Colors.red);

  const _ConnState(this.label, this.color);

  final String label;
  final Color color;
}

class DemoApp extends StatefulWidget {
  const DemoApp({required this.server, required this.client, super.key});

  final GraphqlWsTestServer server;
  final Client client;

  @override
  State<DemoApp> createState() => _DemoAppState();
}

class _DemoAppState extends State<DemoApp> {
  _ConnState _state = _ConnState.connecting;
  int? _count;
  final List<String> _log = [];

  void Function()? _offEvents;
  StreamSubscription<FormattedExecutionResult<Map<String, Object?>, Object?>>?
      _counter;

  @override
  void initState() {
    super.initState();

    // Drive the status indicator and event log off the client's lifecycle
    // events. Reconnection is automatic — these are purely observational.
    _offEvents = widget.client.on<ClientEvent>((event) {
      switch (event) {
        case ConnectingEvent(:final isRetry):
          _set(isRetry ? _ConnState.reconnecting : _ConnState.connecting);
          _append(isRetry ? 'reconnecting…' : 'connecting…');
        case ConnectedEvent(:final wasRetry):
          _set(_ConnState.connected);
          _append(wasRetry ? 'reconnected' : 'connected');
        case ClosedEvent(:final code):
          _set(_ConnState.disconnected);
          _append('socket closed (code $code)');
        case _:
          break;
      }
    });

    // The counter subscription survives reconnects automatically — the
    // client resubscribes for us, so the value just keeps climbing.
    _counter = widget.client
        .stream<Map<String, Object?>, Object?>(
          const SubscribePayload(
            query: 'subscription { count }',
            operationName: 'counter',
          ),
        )
        .listen(
          (result) => setState(() => _count = result.data?['count'] as int?),
          onError: (Object _) {},
        );
  }

  @override
  void dispose() {
    unawaited(_counter?.cancel());
    _offEvents?.call();
    unawaited(widget.client.dispose());
    unawaited(widget.server.dispose());
    super.dispose();
  }

  void _set(_ConnState state) => setState(() => _state = state);

  void _append(String message) {
    final now = DateTime.now();
    String two(int n) => n.toString().padLeft(2, '0');
    setState(() {
      _log.insert(
          0,
          '${two(now.hour)}:${two(now.minute)}:${two(now.second)}'
          '  $message');
      if (_log.length > 40) _log.removeLast();
    });
  }

  @override
  Widget build(BuildContext context) {
    // GraphqlWsConnectionScope keeps the connection healthy across app
    // lifecycle and connectivity changes. `onRecovery` is diagnostic only.
    return GraphqlWsConnectionScope(
      client: widget.client,
      onRecovery: (trigger) => _append('recovery → ${trigger.name}'),
      child: MaterialApp(
        title: 'graphql_ws_flutter demo',
        theme: ThemeData(colorSchemeSeed: Colors.indigo, useMaterial3: true),
        home: Scaffold(
          appBar: AppBar(title: const Text('graphql_ws_flutter demo')),
          body: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                _StatusChip(state: _state),
                const SizedBox(height: 16),
                _CounterCard(count: _count),
                const SizedBox(height: 16),
                Row(
                  children: [
                    Expanded(
                      child: OutlinedButton.icon(
                        onPressed: widget.server.killActiveConnections,
                        icon: const Icon(Icons.wifi_off),
                        label: const Text('Drop connection'),
                      ),
                    ),
                    const SizedBox(width: 12),
                    Expanded(
                      child: OutlinedButton.icon(
                        onPressed: widget.client.terminate,
                        icon: const Icon(Icons.power_settings_new),
                        label: const Text('Terminate'),
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 8),
                const Text(
                  'Drop or terminate the connection, or background the app '
                  'for a few seconds — the counter keeps climbing once the '
                  'client reconnects.',
                  style: TextStyle(fontSize: 12, color: Colors.black54),
                ),
                const SizedBox(height: 16),
                const Text('Event log',
                    style: TextStyle(fontWeight: FontWeight.bold)),
                const Divider(),
                Expanded(child: _EventLog(entries: _log)),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _StatusChip extends StatelessWidget {
  const _StatusChip({required this.state});

  final _ConnState state;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(vertical: 12),
      decoration: BoxDecoration(
        color: state.color.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(Icons.circle, size: 12, color: state.color),
          const SizedBox(width: 8),
          Text(state.label,
              style:
                  TextStyle(color: state.color, fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }
}

class _CounterCard extends StatelessWidget {
  const _CounterCard({required this.count});

  final int? count;

  @override
  Widget build(BuildContext context) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.symmetric(vertical: 24),
        child: Column(
          children: [
            const Text('subscription { count }',
                style: TextStyle(fontFamily: 'monospace', fontSize: 12)),
            const SizedBox(height: 8),
            Text(
              count?.toString() ?? '—',
              style: const TextStyle(fontSize: 56, fontWeight: FontWeight.bold),
            ),
          ],
        ),
      ),
    );
  }
}

class _EventLog extends StatelessWidget {
  const _EventLog({required this.entries});

  final List<String> entries;

  @override
  Widget build(BuildContext context) {
    if (entries.isEmpty) {
      return const Center(child: Text('No events yet.'));
    }
    return ListView.builder(
      itemCount: entries.length,
      itemBuilder: (context, i) => Padding(
        padding: const EdgeInsets.symmetric(vertical: 2),
        child: Text(
          entries[i],
          style: const TextStyle(fontFamily: 'monospace', fontSize: 12),
        ),
      ),
    );
  }
}
