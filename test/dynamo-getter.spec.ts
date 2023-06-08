import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { DynamoGetter } from '../src/dynamo-getter';
import { DynamoTypeFrom } from '../src';
import { complexTableDefinition } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/'  },
  region: 'local-env',
});
const dynamoClient = DynamoDBDocument.from(dynamo);

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
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
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
