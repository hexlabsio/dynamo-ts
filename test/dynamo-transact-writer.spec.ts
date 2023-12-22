import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoTransactWriter } from '../src/dynamo-transact-writer';
import { transactionTableDefinition } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

const transactionTableName = 'transactionTableDefinition';
const transactionTable = new DynamoTransactWriter<
  typeof transactionTableDefinition
>({
  tableName: transactionTableName,
  client: dynamoClient,
  logStatements: true,
});

describe('Transact Writer', () => {
  it('should put an item them update another', async () => {
    await transactionTable
      .put({
        item: { identifier: '777', count: 1, description: 'some description' },
        condition: (compare) => compare().description.notExists,
      })
      .then(
        transactionTable.update({
          key: { identifier: '777-000' },
          increments: [{ key: 'count', start: 0 }],
          updates: { count: 5 },
        }),
      )
      .execute();
    const result = await dynamoClient.scan({
      TableName: transactionTableName,
    });
    expect(result.Items).toEqual([
      {
        count: 1,
        identifier: '777',
        description: 'some description',
      },
      {
        count: 5,
        identifier: '777-000',
      },
    ]);
  });
});
