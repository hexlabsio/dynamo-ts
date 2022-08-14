import { DynamoDB } from 'aws-sdk';
import { DynamoDeleter } from '../src/dynamo-deleter';
import { DynamoTypeFrom } from '../src';
import { complexTableDefinition } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

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
      preInserts.map((Item) => dynamoClient.put({ TableName, Item }).promise()),
    );
  });

  describe('Delete', () => {
    it('should delete item', async () => {
      const hash = 'delete-item-test';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        .promise();
      expect((before.Item as any).hash).toEqual(hash);
      await testTable.delete({ hash });
      const after = await dynamoClient
        .get({ TableName, Key: { hash }, ConsistentRead: true })
        .promise();
      expect(after.Item).toBeUndefined();
    });

    it('should delete item and return old value', async () => {
      const hash = 'delete-item-test-2';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        .promise();
      expect((before.Item as any).hash).toEqual(hash);
      const deleted = await testTable.delete(
        { hash },
        { returnValues: 'ALL_OLD' },
      );
      const after = await dynamoClient
        .get({ TableName, Key: { hash }, ConsistentRead: true })
        .promise();
      expect(after.Item).toBeUndefined();
      expect(deleted.item!.hash).toEqual(hash);
    });

    it('should throw when condition not met', async () => {
      const hash = 'delete-item-test-3';
      const before = await dynamoClient
        .get({ TableName, Key: { hash } })
        .promise();
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
        .promise();
      expect((before.Item as any).hash).toEqual(hash);
      await testTable.delete(
        { hash },
        { condition: (compare) => compare().text.eq('some text') },
      );
      const after = await dynamoClient
        .get({ TableName, Key: { hash }, ConsistentRead: true })
        .promise();
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
