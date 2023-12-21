import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { TableClient } from '../src';
import { complexTableDefinitionFilter, NestedTable } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

const testTable = new TableClient(complexTableDefinitionFilter, {
  tableName: 'complexTableDefinitionFilter',
  client: dynamoClient,
  logStatements: true,
});

const itemMinimal: NestedTable = {
  hash: 'hash1',
  string: 'string required',
  number: 10,
  boolean: true,
  nestedObject: { name: 'example nested name' },
  nestedObjectMultiple: { nestedObject: { name: 'example deep nested name' } },
  nestedObjectChildOptional: {},
  arrayString: ['item one', 'item two'],
  arrayObject: [{ name: 'item one' }, { name: 'item two' }],
  nestedArrayString: { items: ['item one', 'item two'] },
  nestedArrayObject: { items: [{ name: 'item one' }, { name: 'item two' }] },
  mapType: { name: 'example string' },
  listType: ['item one', 'item two'],
};

const itemWithOptionals: NestedTable = {
  hash: 'hash2',
  string: 'string required',
  stringOptional: 'string optional',
  number: 10,
  numberOptional: 100,
  boolean: false,
  booleanOptional: true,
  nestedObject: { name: 'example nested name' },
  nestedObjectMultiple: { nestedObject: { name: 'example deep nested name' } },
  nestedObjectMultipleOptional: {
    nestedObject: { name: 'example deep nested name' },
  },
  nestedObjectOptional: { name: 'example nested name optional' },
  nestedObjectChildOptional: { name: 'example nested name optional' },
  nestedObjectOptionalChildOptional: { name: 'example nested name optional' },
  arrayString: ['item one', 'item two'],
  arrayStringOptional: ['item one', 'item two'],
  arrayObject: [{ name: 'item one' }, { name: 'item two' }],
  arrayObjectOptional: [{ name: 'item one' }, { name: 'item two' }],
  nestedArrayString: { items: ['item one', 'item two'] },
  nestedArrayStringOptional: { items: ['item one', 'item two'] },
  nestedArrayObject: { items: [{ name: 'item one' }, { name: 'item two' }] },
  nestedArrayObjectOptional: {
    items: [{ name: 'item one' }, { name: 'item two' }],
  },
  mapType: { object: { name: 'example string' } },
  listType: [{ name: 'item one' }, { name: ['item two', 'item three'] }],
};

const preInserts: NestedTable[] = [itemMinimal, itemWithOptionals];

describe('Dynamo Nested Filter', () => {
  const TableName = 'complexTableDefinitionFilter';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
  });

  describe('top level primitive equals', () => {
    it('string top level equals', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().string.eq('string required'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('string top level not equals', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().not(compare().string.eq('')),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('boolean top level equals true', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().boolean.eq(true),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('boolean top level equals false', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().boolean.eq(false),
      });
      expect(result.member).toEqual([itemWithOptionals]);
    });

    it('number top level equals', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().number.eq(10),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });
  });

  describe('top level primitive operations', () => {
    it('string top level contains', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().string.contains('required'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('string top level exists', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().stringOptional.exists,
      });
      expect(result.member[0]).toEqual(itemWithOptionals);
    });

    it('string top level not exists', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().stringOptional.notExists,
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('number top level is between', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().number.between(1, 20),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('number top level is not between', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().number.between(200, 300),
      });
      expect(result.member).toEqual([]);
    });

    it('string top level begins with', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().string.beginsWith('string'),
      });
      expect(result.member).toEqual([itemMinimal, itemWithOptionals]);
    });
  });

  describe('top level primitive optional equals', () => {
    it('string optional top level equals', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().stringOptional.eq('string optional'),
      });
      expect(result.member).toEqual([itemWithOptionals]);
    });

    it('boolean optional top level equals true', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().booleanOptional.eq(true),
      });
      expect(result.member).toEqual([itemWithOptionals]);
    });

    it('number optional top level equals', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().numberOptional.eq(100),
      });
      expect(result.member).toEqual([itemWithOptionals]);
    });
  });

  describe('object equals', () => {
    it('top level object equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObject.eq({ name: 'example nested name' }),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('top level object string equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObject.name.eq('example nested name'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('top level optional object string equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObjectOptional.name.eq(
            'example nested name optional',
          ),
      });
      expect(result.member[0]).toEqual(itemWithOptionals);
    });

    it('top level object string optional equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObjectChildOptional.name.eq(
            'example nested name optional',
          ),
      });
      expect(result.member[0]).toEqual(itemWithOptionals);
    });

    it('top level optional object string optional equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObjectOptionalChildOptional.name.eq(
            'example nested name optional',
          ),
      });
      expect(result.member[0]).toEqual(itemWithOptionals);
    });

    it('deep nested object equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObjectMultiple.nestedObject.eq({
            name: 'example deep nested name',
          }),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('deep nested string equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObjectMultiple.nestedObject.name.eq(
            'example deep nested name',
          ),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });
  });

  describe('object operations', () => {
    it('top level object exists', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().nestedObject.exists,
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('top level optional object exists', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().nestedObjectOptional.exists,
      });
      expect(result.member[0]).toEqual(itemWithOptionals);
    });

    it('deep nested object exists', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().nestedObjectMultiple.nestedObject.exists,
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('deep nested optional object exists', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedObjectMultipleOptional.nestedObject.exists,
      });
      expect(result.member[0]).toEqual(itemWithOptionals);
    });
  });

  describe('array equals', () => {
    it('array equals', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().arrayString.eq(['item one', 'item two']),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('array string equals and operation', async () => {
      const result = await testTable.scan({
        filter: (compare) => compare().arrayString[0].eq('item one'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('object array equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedArrayString.items.eq(['item one', 'item two']),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('object array string equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedArrayString.items[0].eq('item one'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('object array object equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedArrayObject.items[0].eq({ name: 'item one' }),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('object array object string equals', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedArrayObject.items[0].name.eq('item one'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    it('object array object string contains', async () => {
      const result = await testTable.scan({
        filter: (compare) =>
          compare().nestedArrayObject.items[0].name.contains('item'),
      });
      expect(result.member[0]).toEqual(itemMinimal);
    });

    describe('map', () => {
      it('map equal', async () => {
        const result = await testTable.scan({
          filter: (compare) => compare().mapType.name.eq('example string'),
        });
        expect(result.member[0]).toEqual(itemMinimal);
      });

      it('map nested equals', async () => {
        const result = await testTable.scan({
          filter: (compare) =>
            compare().mapType.object.name.eq('example string'),
        });
        expect(result.member[0]).toEqual(itemWithOptionals);
      });

      it('map nested contains', async () => {
        const result = await testTable.scan({
          filter: (compare) =>
            compare().mapType.object.name.contains('example'),
        });
        expect(result.member[0]).toEqual(itemWithOptionals);
      });
    });

    describe('list', () => {
      it('list equal', async () => {
        const result = await testTable.scan({
          filter: (compare) => compare().listType.eq(['item one', 'item two']),
        });
        expect(result.member[0]).toEqual(itemMinimal);
      });

      it('list nested equals', async () => {
        const result = await testTable.scan({
          filter: (compare) => compare().listType[1].eq('item two'),
        });
        expect(result.member[0]).toEqual(itemMinimal);
      });

      it('list nested object equals', async () => {
        const result = await testTable.scan({
          filter: (compare) => compare().listType[1].name[1].eq('item three'),
        });
        expect(result.member[0]).toEqual(itemWithOptionals);
      });

      it('list nested object contains', async () => {
        const result = await testTable.scan({
          filter: (compare) => compare().listType[1].name[1].contains('three'),
        });
        expect(result.member[0]).toEqual(itemWithOptionals);
      });
    });

    describe('combine', () => {
      it('single and', async () => {
        const result = await testTable.scan({
          filter: (compare) =>
            compare().and(compare().string.eq('string required')),
        });
        expect(result.member[0]).toEqual(itemMinimal);
      });
      it('and and or', async () => {
        const result = await testTable.scan({
          filter: (compare) =>
            compare().and(
              compare()
                .string.eq('string john')
                .or(compare().string.eq('string required')),
              compare().number.eq(10).and(compare().numberOptional.eq(100)),
            ),
        });
        expect(result.member).toEqual([itemWithOptionals]);
      });
    });
  });
});
