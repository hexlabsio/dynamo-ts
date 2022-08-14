import { DynamoDB } from 'aws-sdk';
import { DynamoGetter } from '../src/dynamo-getter';
import { DynamoTypeFrom } from '../src/types';
import { complexTableDefinition } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoTypeFrom<typeof complexTableDefinition>;

const testTable = new DynamoGetter(complexTableDefinition, {
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const preInserts: TableType[] = [
  { hash: 'get-item-test', text: 'some text', obj: { abc: 'xyz', def: 2 } },
  { hash: 'get-item-test-2', text: 'some other text' },
];

describe('Dynamo Getter', () => {
  const TableName = 'complexTableDefinition';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item }).promise()),
    );
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
});
