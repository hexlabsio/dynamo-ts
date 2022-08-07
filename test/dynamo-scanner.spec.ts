import { DynamoDB } from 'aws-sdk';
import TableClient from '../src/table-client';
import { DynamoType } from '../src/types';
import { simpleTableDefinition2 } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoType<typeof simpleTableDefinition2>;

const testTable = new TableClient(simpleTableDefinition2, {
  tableName: 'simpleTableDefinition2',
  client: dynamoClient,
  logStatements: true,
});
const preInserts: TableType[] = new Array(1000).fill(0).map((a, index) => ({identifier: index.toString(), text: index.toString(), sort: index.toString().padStart(6, '0')}));

describe('Dynamo Scanner', () => {

  const TableName = 'simpleTableDefinition2';

  beforeAll(async () => {
    await Promise.all(preInserts.map(Item => dynamoClient.put({TableName, Item}).promise()));
  });

  it('should scan table', async () => {
    const result = await testTable.scan();
    const ordered = result.member.sort((a,b) => a.sort.localeCompare(b.sort));
    expect(ordered).toEqual(preInserts);
  });

});