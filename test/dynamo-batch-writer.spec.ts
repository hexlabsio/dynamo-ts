import { DynamoDB } from 'aws-sdk';
import TableClient from '../src/table-client';
import { DynamoType } from '../src/types';
import { simpleTableDefinition, simpleTableDefinition2 } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoType<typeof simpleTableDefinition>;
type TableType2 = DynamoType<typeof simpleTableDefinition2>;

const testTable = new TableClient(simpleTableDefinition, {
  tableName: 'simpleTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const testTable2 = new TableClient(simpleTableDefinition2, {
  tableName: 'simpleTableDefinition2',
  client: dynamoClient,
  logStatements: true,
});

const preInserts: TableType[] = new Array(1000).fill(0).map((a, index) => ({identifier: index.toString(), text: index.toString()}));
const preInserts2: TableType2[] = new Array(1000).fill(0).map((a, index) => ({identifier: (10000 + index).toString(), sort: index.toString(), text: 'test'}));

describe('Dynamo Getter', () => {

  const TableName = 'simpleTableDefinition';
  // const TableName2 = 'simpleTableDefinition2';

  beforeAll(async () => {
    await Promise.all(preInserts.map(Item => dynamoClient.put({TableName, Item}).promise()));
  });

  describe('Single Table', () => {
    it('should batch put single table', async () => {
      const executor = testTable.batchPut(preInserts.slice(0, 25));
      console.log(JSON.stringify(executor.input, null, 2));
      await executor.execute();
    });
  });


  describe('Multi Table', () => {
    it('should batch put multi table', async () => {
      const executor = testTable.batchDelete(preInserts.slice(0, 2).map(({identifier}) => ({identifier})))
        .and(testTable.batchPut(preInserts.slice(2, 4)))
        .and(testTable2.batchPut(preInserts2.slice(0, 2)));
      console.log(JSON.stringify(executor.input, null, 2));
      await executor.execute();
    });
  });

});