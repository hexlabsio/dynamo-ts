import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoGetter, DynamoPuter } from '../src';
import {
  complexTableDefinition,
} from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

const testTablePuter = new DynamoPuter<typeof complexTableDefinition>({
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

const testTableGetter = new DynamoGetter<typeof complexTableDefinition>({
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

describe('Dynamo Puter', () => {

  describe('Put', () => {
    it('should put item', async () => {
      await testTablePuter.put({hash: 'put-item-test', jkl: 90 });
      const result = await testTableGetter.get({hash: 'put-item-test'})
      expect(result.item).toEqual({
        hash: 'put-item-test',
        jkl: 90
      });
    });

    it('should put item 2', async () => {
      await testTablePuter.put({hash: 'put-item-test', jkl: 90 }, {condition: (compare) => compare().obj.abc.notExists });
      const result = await testTableGetter.get({hash: 'put-item-test'})
      expect(result.item).toEqual({
        hash: 'put-item-test',
        jkl: 90
      });
    });
  })
});
