import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient } from '../src';
import { Crud } from '../src/crud';
import { indexTableDefinition } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

const indexTable = new TableClient(indexTableDefinition, {
  tableName: 'indexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

describe('Crud', () => {
  it('should create and get item', async () => {
    const service = new Crud(indexTable);
    const item = await service.create({ sort: 'abc', indexHash: 'xyz' });
    const result = await service.read({ hash: item.hash, sort: item.sort });
    expect(result).toEqual({
      hash: item.hash,
      sort: 'abc',
      indexHash: 'xyz',
    });
  });
});
