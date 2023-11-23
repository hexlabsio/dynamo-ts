import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoScanner } from '../src';
import { SimpleTable2, simpleTableDefinition3 } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);


const testTable = new DynamoScanner<typeof simpleTableDefinition3>({
  tableName: 'simpleTableDefinition3',
  client: dynamoClient,
  logStatements: true,
});

const preInserts: SimpleTable2[] = new Array(1000).fill(0).map((a, index) => ({
  identifier: index.toString(),
  text: index.toString(),
  sort: index.toString().padStart(6, '0'),
}));

describe('Dynamo Scanner', () => {
  const TableName = 'simpleTableDefinition3';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
  });

  it('should scan table', async () => {
    const result = await testTable.scan();
    const ordered = result.member.sort((a, b) => a.sort.localeCompare(b.sort));
    expect(ordered).toEqual(preInserts);
  });
});
