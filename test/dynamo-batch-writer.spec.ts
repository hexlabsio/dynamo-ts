import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { TableClient } from '../src';
import {
  SimpleTable,
  SimpleTable2,
  simpleTableDefinition,
  simpleTableDefinition2,
} from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

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

const preInserts: SimpleTable[] = new Array(1000).fill(0).map((a, index) => ({
  identifier: index.toString(),
  sort: index.toString(),
}));
const preInserts2: SimpleTable2[] = new Array(1000).fill(0).map((a, index) => ({
  identifier: (10000 + index).toString(),
  sort: index.toString(),
  text: 'test',
}));

describe('Dynamo Batch Writer', () => {
  const TableName = 'simpleTableDefinition';
  // const TableName2 = 'simpleTableDefinition2';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
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
      const executor = testTable
        .batchDelete(
          preInserts.slice(0, 2).map(({ identifier }) => ({ identifier })),
        )
        .and(testTable.batchPut(preInserts.slice(2, 4)))
        .and(testTable2.batchPut(preInserts2.slice(0, 2)));
      console.log(JSON.stringify(executor.input, null, 2));
      await executor.execute();
    });
  });

  describe('Chunking', () => {
    it('should split requests larger than 25 items', async () => {
      const items = new Array(60).fill(0).map((a, index) => ({
        identifier: `chunk-${index}`,
        sort: `${index}`,
      }));
      const result = await testTable.batchPut(items).execute(true);
      expect(result.unprocessedItems).toEqual({});
      const stored = await testTable
        .batchGet(items.map(({ identifier }) => ({ identifier })))
        .execute();
      expect(stored.items).toEqual(expect.arrayContaining(items));
      expect(stored.items.length).toEqual(60);
    });
  });

  it('should merge requests for the same table from every executor', () => {
    const executor = testTable
      .batchPut(preInserts.slice(0, 1))
      .and(testTable2.batchPut(preInserts2.slice(0, 1)))
      .and(testTable.batchPut(preInserts.slice(1, 2)))
      .and(testTable2.batchPut(preInserts2.slice(1, 2)));
    expect(
      executor.input.RequestItems!['simpleTableDefinition'].length,
    ).toEqual(2);
    expect(
      executor.input.RequestItems!['simpleTableDefinition2'].length,
    ).toEqual(2);
  });
});
