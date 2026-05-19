/// Port of `tests/common.test.ts`.
///
/// Every invalid/valid case from the upstream JS file is mirrored here, plus
/// a few Dart-specific cases (e.g. typed sealed-class round-trips) that the
/// JS version cannot easily express.
@Tags(['common'])
library;

import 'package:graphql_ws/graphql_ws.dart';
import 'package:test/test.dart';

void main() {
  group('validateMessage — invalid', () {
    // Straight-up invalid envelopes (parity with JS `it.each` block 1).
    final straightUpInvalid = <Object?>[
      <String, Object?>{}, // {}
      '', // empty string
      <Object?>[], // []
      0,
      9,
      #aSymbol, // Symbol parity
      Object(), // Object parity
      () {}, // function parity
    ];
    for (var i = 0; i < straightUpInvalid.length; i++) {
      final val = straightUpInvalid[i];
      test('reports invalid envelope #$i (${val.runtimeType})', () {
        expect(() => validateMessage(val), throwsA(isA<FormatException>()));
      });
    }

    // Invalid `type` property.
    test('rejects {type: ""}', () {
      expect(() => validateMessage({'type': ''}),
          throwsA(isA<FormatException>()));
    });
    test('rejects {type: null}', () {
      // JS uses undefined; in Dart we use null. Same validator path triggers.
      expect(() => validateMessage({'type': null}),
          throwsA(isA<FormatException>()));
    });
    test('rejects {type: 0}', () {
      expect(() => validateMessage({'type': 0}),
          throwsA(isA<FormatException>()));
    });
    test('rejects {type: "nuxt"}', () {
      expect(() => validateMessage({'type': 'nuxt'}),
          throwsA(isA<FormatException>()));
    });

    // Invalid connection_init/ack/ping/pong: non-object, non-null payload.
    final scalarPayloadCases = <Map<String, Object?>>[
      {'type': MessageType.connectionInit.wireValue, 'payload': ''},
      {'type': MessageType.connectionInit.wireValue, 'payload': 0},
      {'type': MessageType.connectionAck.wireValue, 'payload': ''},
      {'type': MessageType.ping.wireValue, 'payload': 0},
    ];
    for (var i = 0; i < scalarPayloadCases.length; i++) {
      final v = scalarPayloadCases[i];
      test(
          'rejects ${v['type']} with non-object scalar payload '
          '(${v['payload'].runtimeType})', () {
        expect(() => validateMessage(v), throwsA(isA<FormatException>()));
      });
    }

    // Invalid subscribe message.
    final invalidSubscribes = <Map<String, Object?>>[
      {'type': MessageType.subscribe.wireValue},
      {'id': 0, 'type': MessageType.subscribe.wireValue},
      {'id': '', 'type': MessageType.subscribe.wireValue},
      {'id': 'id', 'type': MessageType.subscribe.wireValue},
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': <Object?>[],
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': '',
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': <String, Object?>{},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'query': 0},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'query': <String, Object?>{}},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'operationName': 0, 'query': ''},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'operationName': <String, Object?>{}, 'query': ''},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'query': '', 'variables': ''},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'query': '', 'extensions': ''},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'query': '', 'extensions': 0},
      },
    ];
    for (var i = 0; i < invalidSubscribes.length; i++) {
      final v = invalidSubscribes[i];
      test('rejects invalid subscribe #$i', () {
        expect(() => validateMessage(v), throwsA(isA<FormatException>()));
      });
    }

    // Invalid next message.
    final invalidNexts = <Map<String, Object?>>[
      {'type': MessageType.next.wireValue},
      {'id': null, 'type': MessageType.next.wireValue},
      {'id': '', 'type': MessageType.next.wireValue},
      {'id': 'id', 'type': MessageType.next.wireValue},
      {'id': 'id', 'type': MessageType.next.wireValue, 'payload': ''},
    ];
    for (var i = 0; i < invalidNexts.length; i++) {
      final v = invalidNexts[i];
      test('rejects invalid next #$i', () {
        expect(() => validateMessage(v), throwsA(isA<FormatException>()));
      });
    }

    // Invalid error message.
    final invalidErrors = <Map<String, Object?>>[
      {'type': MessageType.error.wireValue},
      {'id': '', 'type': MessageType.error.wireValue},
      {'id': 'id', 'type': MessageType.error.wireValue},
      {
        'id': 'id',
        'type': MessageType.error.wireValue,
        'payload': <String, Object?>{},
      },
      {'id': 'id', 'type': MessageType.error.wireValue, 'payload': ''},
      {'id': 'id', 'type': MessageType.error.wireValue, 'payload': <Object?>[]},
      {
        'id': 'id',
        'type': MessageType.error.wireValue,
        'payload': [
          {'iam': 'invalid'}
        ],
      },
    ];
    for (var i = 0; i < invalidErrors.length; i++) {
      final v = invalidErrors[i];
      test('rejects invalid error #$i', () {
        expect(() => validateMessage(v), throwsA(isA<FormatException>()));
      });
    }

    // Invalid complete message.
    final invalidCompletes = <Map<String, Object?>>[
      {'type': MessageType.complete.wireValue},
      {'id': '', 'type': MessageType.complete.wireValue},
      {'id': 0, 'type': MessageType.complete.wireValue},
    ];
    for (var i = 0; i < invalidCompletes.length; i++) {
      final v = invalidCompletes[i];
      test('rejects invalid complete #$i', () {
        expect(() => validateMessage(v), throwsA(isA<FormatException>()));
      });
    }
  });

  group('validateMessage — valid', () {
    final valid = <Map<String, Object?>>[
      // connection_init / ack / ping / pong, with various payload shapes.
      {'type': MessageType.connectionInit.wireValue},
      {
        'type': MessageType.connectionInit.wireValue,
        'payload': <String, Object?>{}
      },
      {'type': MessageType.connectionInit.wireValue, 'payload': null},
      {'type': MessageType.connectionAck.wireValue},
      {
        'type': MessageType.connectionAck.wireValue,
        'payload': <String, Object?>{}
      },
      {'type': MessageType.connectionAck.wireValue, 'payload': null},
      {'type': MessageType.ping.wireValue},
      {'type': MessageType.ping.wireValue, 'payload': <String, Object?>{}},
      {'type': MessageType.ping.wireValue, 'payload': null},
      {'type': MessageType.pong.wireValue},
      {'type': MessageType.pong.wireValue, 'payload': <String, Object?>{}},
      {'type': MessageType.pong.wireValue, 'payload': null},

      // subscribe.
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {'query': ''},
      },
      {
        'id': 'id',
        'type': MessageType.subscribe.wireValue,
        'payload': {
          'operationName': null,
          'variables': null,
          'extensions': null,
          'query': '',
        },
      },

      // error.
      {
        'id': 'id',
        'type': MessageType.error.wireValue,
        'payload': [
          {'message': 'I am Error'}
        ],
      },

      // complete.
      {'id': 'id', 'type': MessageType.complete.wireValue},

      // next with object payload.
      {
        'id': 'id',
        'type': MessageType.next.wireValue,
        'payload': <String, Object?>{},
      },
    ];

    for (var i = 0; i < valid.length; i++) {
      final v = valid[i];
      test('accepts valid #$i (${v['type']})', () {
        expect(() => validateMessage(v), returnsNormally);
      });
    }
  });

  group('parseMessage / stringifyMessage round-trip', () {
    test('round-trips ConnectionInit with payload', () {
      const msg = ConnectionInitMessage(payload: {'auth': 'token'});
      final wire = stringifyMessage(msg);
      final back = parseMessage(wire);
      expect(back, isA<ConnectionInitMessage>());
      expect((back as ConnectionInitMessage).payload, equals({'auth': 'token'}));
    });

    test('round-trips ConnectionInit without payload (no payload key)', () {
      const msg = ConnectionInitMessage();
      final wire = stringifyMessage(msg);
      expect(wire, equals('{"type":"connection_init"}'));
      final back = parseMessage(wire);
      expect(back, isA<ConnectionInitMessage>());
      expect((back as ConnectionInitMessage).payload, isNull);
    });

    test('round-trips Subscribe with operationName/variables/extensions', () {
      final msg = SubscribeMessage(
        id: 'op-1',
        payload: const SubscribePayload(
          query: 'subscription { greetings }',
          operationName: 'Greetings',
          variables: {'name': 'World'},
          extensions: {'tracing': true},
        ),
      );
      final wire = stringifyMessage(msg);
      final back = parseMessage(wire);
      expect(back, isA<SubscribeMessage>());
      back as SubscribeMessage;
      expect(back.id, equals('op-1'));
      expect(back.payload.query, equals('subscription { greetings }'));
      expect(back.payload.operationName, equals('Greetings'));
      expect(back.payload.variables, equals({'name': 'World'}));
      expect(back.payload.extensions, equals({'tracing': true}));
    });

    test('round-trips Next with embedded data + errors + hasNext', () {
      final msg = NextMessage(
        id: 'op-1',
        payload: FormattedExecutionResult<Map<String, Object?>,
            Map<String, Object?>>(
          data: {'hello': 'world'},
          errors: const [
            GraphQLFormattedError(
              message: 'a partial error',
              path: ['hello'],
            ),
          ],
          hasNext: true,
        ),
      );
      final wire = stringifyMessage(msg);
      final back = parseMessage(wire);
      expect(back, isA<NextMessage>());
      back as NextMessage;
      expect(back.id, equals('op-1'));
      expect(back.payload.data, equals({'hello': 'world'}));
      expect(back.payload.hasNext, isTrue);
      expect(back.payload.errors?.first.message, equals('a partial error'));
      expect(back.payload.errors?.first.path, equals(['hello']));
    });

    test('round-trips Error with locations + path + extensions', () {
      final msg = ErrorMessage(
        id: 'op-1',
        payload: const [
          GraphQLFormattedError(
            message: 'Syntax Error',
            locations: [SourceLocation(line: 1, column: 5)],
            path: ['greet', 0, 'name'],
            extensions: {'code': 'SYNTAX'},
          ),
        ],
      );
      final wire = stringifyMessage(msg);
      final back = parseMessage(wire) as ErrorMessage;
      expect(back.id, equals('op-1'));
      expect(back.payload, hasLength(1));
      expect(back.payload.first.message, equals('Syntax Error'));
      expect(back.payload.first.locations,
          equals(const [SourceLocation(line: 1, column: 5)]));
      expect(back.payload.first.path, equals(['greet', 0, 'name']));
      expect(back.payload.first.extensions, equals({'code': 'SYNTAX'}));
    });

    test('round-trips Complete', () {
      const msg = CompleteMessage(id: 'op-1');
      final wire = stringifyMessage(msg);
      expect(wire, equals('{"id":"op-1","type":"complete"}'));
      final back = parseMessage(wire);
      expect(back, isA<CompleteMessage>());
      expect((back as CompleteMessage).id, equals('op-1'));
    });

    test('jsonMessageReviver is applied', () {
      const raw = '{"type":"next","id":"x","payload":{"data":{"v":1}}}';
      final back = parseMessage(raw, reviver: (key, value) {
        if (key == 'v') return 42;
        return value;
      }) as NextMessage;
      expect(back.payload.data, equals({'v': 42}));
    });
  });

  group('FormattedExecutionResult', () {
    test('preserves the data:null vs data:absent distinction', () {
      final absent = FormattedExecutionResult<Map<String, Object?>,
          Map<String, Object?>>.fromJson(const {'errors': []});
      expect(absent.hasData, isFalse);
      expect(absent.data, isNull);

      final explicitNull = FormattedExecutionResult<Map<String, Object?>,
          Map<String, Object?>>.fromJson(const {
        'data': null,
        'errors': [
          {'message': 'failed'}
        ],
      });
      expect(explicitNull.hasData, isTrue);
      expect(explicitNull.data, isNull);
    });

    test('errors getter returns typed list', () {
      final r = FormattedExecutionResult<Map<String, Object?>,
          Map<String, Object?>>.fromJson(const {
        'errors': [
          {'message': 'boom', 'path': ['x', 0]}
        ],
      });
      expect(r.errors, hasLength(1));
      expect(r.errors!.first.message, equals('boom'));
      expect(r.errors!.first.path, equals(['x', 0]));
    });
  });

  group('SubscribePayload', () {
    test('toJson omits null fields and preserves query', () {
      const p = SubscribePayload(query: '{ getValue }');
      expect(p.toJson(), equals({'query': '{ getValue }'}));
    });

    test('round-trips through fromJson', () {
      final json = {
        'query': '{ x }',
        'operationName': 'X',
        'variables': {'a': 1},
        'extensions': {'e': true},
      };
      final p = SubscribePayload.fromJson(json);
      expect(p.query, equals('{ x }'));
      expect(p.operationName, equals('X'));
      expect(p.variables, equals({'a': 1}));
      expect(p.extensions, equals({'e': true}));
    });
  });
}
