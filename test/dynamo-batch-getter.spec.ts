import { DynamoDB } from 'aws-sdk';
import { DynamoTypeFrom, TableClient } from '../src';
import { simpleTableDefinition, simpleTableDefinition2 } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoTypeFrom<typeof simpleTableDefinition>;
type TableType2 = DynamoTypeFrom<typeof simpleTableDefinition2>;

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

const preInserts: TableType[] = new Array(1000).fill(0).map((a, index) => ({
  identifier: index.toString(),
  text: index.toString(),
}));
const preInserts2: TableType2[] = new Array(1000).fill(0).map((a, index) => ({
  identifier: (10000 + index).toString(),
  sort: index.toString(),
  text: 'test',
}));

describe('Dynamo Getter', () => {
  const TableName = 'simpleTableDefinition';
  const TableName2 = 'simpleTableDefinition2';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item }).promise()),
    );
    await Promise.all(
      preInserts2.map((Item) =>
        dynamoClient.put({ TableName: TableName2, Item }).promise(),
      ),
    );
  }, 20000);

  describe('Single Table', () => {
    it('should batch get single table', async () => {
      const executor = testTable.batchGet([
        { identifier: '0' },
        { identifier: '3' },
        { identifier: '4' },
      ]);
      console.log(JSON.stringify(executor.input, null, 2));
      const result = await executor.execute();
      expect(result.items).toEqual([
        { identifier: '0', text: '0' },
        { identifier: '3', text: '3' },
        { identifier: '4', text: '4' },
      ]);
    });
  });

  describe('Multi Table', () => {
    it('should batch get multi table', async () => {
      const result = await testTable
        .batchGet([
          { identifier: '0' },
          { identifier: '3' },
          { identifier: '4' },
        ])
        .and(
          testTable2.batchGet(
            [
              { identifier: '10000', sort: '0' },
              { identifier: '10008', sort: '8' },
            ],
            { projection: (projector) => projector.project('sort') },
          ),
        )
        .execute();
      expect(result.items).toEqual([
        [
          { identifier: '0', text: '0' },
          { identifier: '3', text: '3' },
          { identifier: '4', text: '4' },
        ],
        [{ sort: '8' }, { sort: '0' }],
      ]);
    });
  });
});
