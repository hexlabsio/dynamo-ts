import { DynamoDB } from 'aws-sdk';
import { TableClient } from '../src/table-client';
import { complexTableDefinition } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

const testTable = TableClient.build(complexTableDefinition, {
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

describe('Dynamo Table', () => {
  beforeAll(async () => {
    await testTable.put({
      hash: 'get-item-test',
      text: 'some text',
      obj: { abc: 'xyz', def: 2 },
    });
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
        { ReturnConsumedCapacity: 'TOTAL' },
      );
      expect(result.consumedCapacity).toEqual({
        TableName: 'complexTableDefinition',
        CapacityUnits: 0.5,
      });
    });
    it('should allow consistent read', async () => {
      const result = await testTable.get(
        { hash: 'get-item-test' },
        { ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true },
      );
      expect(result.consumedCapacity).toEqual({
        TableName: 'complexTableDefinition',
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
