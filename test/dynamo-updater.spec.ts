import { DynamoDB } from 'aws-sdk';
import { TableClient } from '../src';
import { complexTableDefinition } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:8000',
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

describe('Dynamo Updater', () => {
  beforeAll(async () => {
    await testTable.put({
      hash: 'update-item-test',
      text: 'some text',
      obj: { abc: 'def', def: 2 },
    });
  });
  describe('Simple Update', () => {
    it('should update item', async () => {
      const result = await testTable.update({
        key: { hash: 'update-item-test' },
        updates: { text: 'updated', obj: undefined },
        return: 'ALL_NEW',
      });
      expect(result.item).toEqual({
        hash: 'update-item-test',
        text: 'updated',
      });
    });
  });
});
