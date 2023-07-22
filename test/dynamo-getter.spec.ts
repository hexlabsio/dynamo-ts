import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoGetter } from '../src/dynamo-getter';
import { DynamoTypeFrom, TableClient } from '../src';
import {
  binaryTableDefinition,
  complexTableDefinition,
  setsTableDefinition,
} from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

type TableType = DynamoTypeFrom<typeof complexTableDefinition>;
type SetTableType = DynamoTypeFrom<typeof setsTableDefinition>;
type BinaryTableType = DynamoTypeFrom<typeof binaryTableDefinition>;

const testTable = new DynamoGetter(complexTableDefinition, {
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const setTable = new TableClient(setsTableDefinition, {
  tableName: 'setsTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const binaryTable = new TableClient(binaryTableDefinition, {
  tableName: 'binaryTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const preInserts: TableType[] = [
  { hash: 'get-item-test', text: 'some text', obj: { abc: 'xyz', def: 2 } },
  { hash: 'get-item-test-2', text: 'some other text' },
];

const setPreInserts: SetTableType[] = [
  {
    identifier: 'get-set-item-test',
    uniqueNumbers: new Set([1, 1, 2, 3]),
    uniqueStrings: new Set(['a', 'b', 'b']),
  },
];

const binaryPreInserts: BinaryTableType[] = [
  { identifier: 'get-bin-item-test', bin: Buffer.from('hello world') },
];

describe('Dynamo Getter', () => {
  const TableName = 'complexTableDefinition';

  beforeAll(async () => {
    await Promise.all([
      ...preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
      ...setPreInserts.map((Item) =>
        dynamoClient.put({ TableName: 'setsTableDefinition', Item }),
      ),
      ...binaryPreInserts.map((Item) =>
        dynamoClient.put({ TableName: 'binaryTableDefinition', Item }),
      ),
    ]);
  });

  describe('Get', () => {
    it('should get item', async () => {
      const result = await testTable.get({ hash: 'get-item-test' });
      expect(result.item).toEqual({
        hash: 'get-item-test',
        text: 'some text',
        obj: { abc: 'xyz', def: 2 },
      });
    });

    it('should return consumed capacity', async () => {
      const result = await testTable.get(
        { hash: 'get-item-test' },
        { returnConsumedCapacity: 'TOTAL' },
      );
      expect(result.consumedCapacity).toEqual({
        TableName,
        CapacityUnits: 0.5,
      });
    });

    it('should allow consistent read', async () => {
      const result = await testTable.get(
        { hash: 'get-item-test' },
        { returnConsumedCapacity: 'TOTAL', consistentRead: true },
      );
      expect(result.consumedCapacity).toEqual({
        TableName,
        CapacityUnits: 1,
      });
    });

    it('should project result', async () => {
      const result = await testTable.get(
        { hash: 'get-item-test' },
        { projection: (projector) => projector.project('obj.abc') },
      );
      expect(result.item).toEqual({ obj: { abc: 'xyz' } });
      expect(result.item!.obj.abc).toEqual('xyz'); //verifies that obj.abc exists on type of item
    });
  });

  describe('Sets', () => {
    it('should get unique items from sets', async () => {
      const result = await setTable.get({ identifier: 'get-set-item-test' });
      expect(result.item!.uniqueNumbers.size).toEqual(3);
      expect([...result.item!.uniqueNumbers]).toEqual([1, 2, 3]);
      expect(result.item!.uniqueStrings.size).toEqual(2);
      expect([...result.item!.uniqueStrings]).toEqual(['a', 'b']);
    });
  });

  describe('Binary', () => {
    it('should get binary items as buffers', async () => {
      const result = await binaryTable.get({ identifier: 'get-bin-item-test' });
      expect(
        Buffer.from(result.item?.bin!, 'base64').toString('utf-8'),
      ).toEqual('hello world');
    });
  });
});
