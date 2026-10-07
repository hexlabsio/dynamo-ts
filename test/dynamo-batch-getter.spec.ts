import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { BatchGetClient, TableClient } from '../src';
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
  tableName: 'simpleTableDefinitionBatch',
  client: dynamoClient,
  logStatements: true,
});

const testTable2 = new TableClient(simpleTableDefinition2, {
  tableName: 'simpleTableDefinitionBatch2',
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

describe('Dynamo Batch Getter', () => {
  const TableName = 'simpleTableDefinitionBatch';
  const TableName2 = 'simpleTableDefinitionBatch2';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
    await Promise.all(
      preInserts2.map((Item) =>
        dynamoClient.put({ TableName: TableName2, Item }),
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
        { identifier: '0', sort: '0' },
        { identifier: '3', sort: '3' },
        { identifier: '4', sort: '4' },
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
          { identifier: '0', sort: '0' },
          { identifier: '3', sort: '3' },
          { identifier: '4', sort: '4' },
        ],
        [{ sort: '8' }, { sort: '0' }],
      ]);
    });
  });

  describe('Reprocessing', () => {
    it('should complete when there are no unprocessed keys', async () => {
      const result = await testTable
        .batchGet([{ identifier: '0' }])
        .and(testTable2.batchGet([{ identifier: '10000', sort: '0' }]))
        .execute(true);
      expect(result.items).toEqual([
        [{ identifier: '0', sort: '0' }],
        [{ identifier: '10000', sort: '0', text: 'test' }],
      ]);
      expect(result.unprocessedKeys).toEqual({});
    });

    it('should retry unprocessed keys', async () => {
      const calls: any[] = [];
      const client = {
        batchGet: async (input: any) => {
          calls.push(input);
          if (calls.length === 1) {
            return {
              Responses: { table: [{ identifier: '0' }] },
              UnprocessedKeys: {
                table: { Keys: [{ identifier: '1' }] },
              },
            };
          }
          return { Responses: { table: [{ identifier: '1' }] } };
        },
      } as unknown as DynamoDBDocument;
      const executor = new TableClient(simpleTableDefinition, {
        tableName: 'table',
        client,
      }).batchGet([{ identifier: '0' }, { identifier: '1' }]);
      const result = await new BatchGetClient(client, [executor]).execute(true);
      expect(calls.length).toEqual(2);
      expect(calls[1].RequestItems).toEqual({
        table: { Keys: [{ identifier: '1' }] },
      });
      expect(result.items).toEqual([
        [{ identifier: '0' }, { identifier: '1' }],
      ]);
      expect(result.unprocessedKeys).toEqual({});
    });
  });

  describe('Chunking', () => {
    it('should split requests larger than 100 keys', async () => {
      const keys = preInserts
        .slice(0, 250)
        .map(({ identifier }) => ({ identifier }));
      const result = await testTable.batchGet(keys).execute();
      expect(result.items.length).toEqual(250);
      expect(result.items).toEqual(
        expect.arrayContaining(preInserts.slice(0, 250)),
      );
    });
  });

  it('should reject multiple requests for the same table', () => {
    expect(() =>
      testTable
        .batchGet([{ identifier: '0' }])
        .and(testTable.batchGet([{ identifier: '1' }])),
    ).toThrow(/already contains a request for table/);
  });
});
