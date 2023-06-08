import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { DynamoDeleter } from '../src/dynamo-deleter';
import { DynamoTypeFrom } from '../src';
import { complexTableDefinition } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/'  },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' }
});
const dynamoClient = DynamoDBDocument.from(dynamo);
type TableType = DynamoTypeFrom<typeof complexTableDefinition>;

const testTable = new DynamoDeleter(complexTableDefinition, {
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const preInserts: TableType[] = [
  { hash: 'delete-item-test', text: 'some text', obj: { abc: 'xyz', def: 2 } },
  {
    hash: 'delete-item-test-2',
    text: 'some text',
    obj: { abc: 'xyz', def: 2 },
  },
  {
    hash: 'delete-item-test-3',
    text: 'some text',
    obj: { abc: 'xyz', def: 2 },
  },
  { hash: 'delete-item-test-4', text: 'some other text' },
  { hash: 'delete-item-test-5', text: 'some other text 2' },
];

describe('Dynamo Deleter', () => {
  const TableName = 'complexTableDefinition';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
  });

  describe('Delete', () => {
    it('should delete item', async () => {
      const hash = 'delete-item-test';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        ;
      expect((before.Item as any).hash).toEqual(hash);
      await testTable.delete({ hash });
      const after = await dynamoClient
        .get({ TableName, Key: { hash }, ConsistentRead: true })
        ;
      expect(after.Item).toBeUndefined();
    });

    it('should delete item and return old value', async () => {
      const hash = 'delete-item-test-2';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        ;
      expect((before.Item as any).hash).toEqual(hash);
      const deleted = await testTable.delete(
        { hash },
        { returnValues: 'ALL_OLD' },
      );
      const after = await dynamoClient
        .get({ TableName, Key: { hash }, ConsistentRead: true })
        ;
      expect(after.Item).toBeUndefined();
      expect(deleted.item!.hash).toEqual(hash);
    });

    it('should throw when condition not met', async () => {
      const hash = 'delete-item-test-3';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        ;
      expect((before.Item as any).hash).toEqual(hash);
      await expect(
        testTable.delete(
          { hash },
          { condition: (compare) => compare().text.eq('not this') },
        ),
      ).rejects.toThrow('The conditional request failed');
    });

    it('should succeed when deleting item that does not exist', async () => {
      const hash = 'delete-item-test-not-present';
      const result = await testTable.delete(
        { hash },
        { returnValues: 'ALL_OLD' },
      );
      expect(result.item).toBeUndefined();
    });

    it('should delete item when condition met', async () => {
      const hash = 'delete-item-test-3';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        ;
      expect((before.Item as any).hash).toEqual(hash);
      await testTable.delete(
        { hash },
        { condition: (compare) => compare().text.eq('some text') },
      );
      const after = await dynamoClient
        .get({ TableName, Key: { hash }, ConsistentRead: true })
        ;
      expect(after.Item).toBeUndefined();
    });

    it('should return consumed capacity', async () => {
      const hash = 'delete-item-test-4';
      const result = await testTable.delete(
        { hash },
        { returnConsumedCapacity: 'TOTAL' },
      );
      expect(result.consumedCapacity).toEqual({
        CapacityUnits: 1,
        TableName,
      });
    });
  });
});
