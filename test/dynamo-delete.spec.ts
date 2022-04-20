import { DynamoDB } from 'aws-sdk';
import { TableClient } from '../src/table-client';
import { deleteTableDefinition } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

const testTable = TableClient.build(deleteTableDefinition, {
  tableName: 'deleteTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

describe('Dynamo Delete', () => {
  beforeEach(async () => {
    await testTable.put({
      hash: 'get-item-test',
      text: 'some text',
      obj: { abc: 'xyz', def: 2 },
    });
  });
  afterEach(async () => {
    await testTable.delete({ hash: 'get-item-test' });
  });

  describe('Delete', () => {
    it('should delete item', async () => {
      await testTable.delete({ hash: 'get-item-test' });
      expect((await testTable.scan()).member.length).toEqual(0);
    });
    it('should not delete item', async () => {
      await testTable.delete({ hash: 'invalid hash key' });
      expect((await testTable.scan()).member.length).toEqual(1);
    });
    it('should delete item conditionally', async () => {
      await testTable.delete({ hash: 'get-item-test' }, { condition: (compare) => compare().exists('text')});
      expect((await testTable.scan()).member.length).toEqual(0);
    });
    it('should not delete item conditionally', async () => {
      await expect(async () => {
        await testTable.delete({ hash: 'get-item-test' }, { condition: (compare) => compare().notExists('text')});
      }).rejects.toThrow('The conditional request failed');
    });
    it('should delete item conditionally (complex)', async () => {
      await testTable.delete({ hash: 'get-item-test' }, { condition: (compare) => compare().exists('text').and(compare().text!.eq('some text'))});
      expect((await testTable.scan()).member.length).toEqual(0);
    });
    it('should not delete item conditionally (complex)', async () => {
      await expect(async () => {
        await testTable.delete({ hash: 'get-item-test' }, { condition: (compare) => compare().exists('text').and(compare().text!.eq('invalid'))});
      }).rejects.toThrow('The conditional request failed');
    });
  });
});
