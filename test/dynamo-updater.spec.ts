import { DynamoDB } from 'aws-sdk';
import { DynamoUpdater } from '../src/dynamo-updater';
import { DynamoType } from '../src/types';
import { complexTableDefinitionQuery } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoType<typeof complexTableDefinitionQuery>;

const TableName = 'complexTableDefinitionQuery';

const testTable = new DynamoUpdater(complexTableDefinitionQuery, {
  tableName: TableName,
  client: dynamoClient,
  logStatements: true,
});


const hash = 'update-items-test';

const preInserts: TableType[] = [
  { hash, text: 'some text', obj: { abc: 'xyz', def: 2 }, mno: 2, pqr: 'yyy'  },
  { hash: hash + '2', text: 'some other text', mno: 'abc', pqr: '123 456' },
];


describe('Dynamo Updater', () => {

  beforeAll(async () => {
    await Promise.all(preInserts.map(Item => dynamoClient.put({TableName, Item}).promise()));
  });

  describe('Key conditions', () => {

    it('should find single item by partition', async () => {
      const result = await testTable.update({ key: { hash }, updates: { text: 'test' }, return: 'ALL_NEW' });
      expect(result.item).toEqual({ ...preInserts[0], text: 'test' });
    });

  });
});