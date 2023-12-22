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

describe('Transact Getter', () => {
  const TableName = 'simpleTableDefinitionBatch';
  const TableName2 = 'simpleTableDefinitionBatch2';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({TableName, Item})),
    );
    await Promise.all(
      preInserts2.map((Item) =>
        dynamoClient.put({TableName: TableName2, Item}),
      ),
    );
  }, 20000);

  describe('Single Table', () => {

    it('should get single table', async () => {
      const executor =
        testTable.transaction.get([{identifier: '0'}])
        .and(
          testTable.transaction.get([{identifier: '3'}])
        )
        .and(
          testTable.transaction.get([{identifier: '4'}])
        );
      console.log(JSON.stringify(executor.input, null, 2));
      const result = await executor.execute();
      expect(result.items).toEqual([
        {identifier: '0', sort: '0'},
        {identifier: '3', sort: '3'},
        {identifier: '4', sort: '4'},
      ]);
    });

    it('should get multi table', async () => {
      const executor =
        testTable.transaction.get([{identifier: '0'}])
        .and(
          testTable2.transaction.get([{identifier: '10000', sort: '0'}])
        )
      console.log(JSON.stringify(executor.input, null, 2));
      const result = await executor.execute();
      expect(result.items).toEqual([
        {identifier: '0', sort: '0'},
        {identifier: '10000', sort: '0', text: 'test'},
      ]);
    });
  });
});
