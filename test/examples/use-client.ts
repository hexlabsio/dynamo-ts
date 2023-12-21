import { DynamoConfig, TableClient } from '../../src';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { myTableDefinition } from './define-table';

const dynamoConfig: DynamoConfig = {
  client: DynamoDBDocument.from(new DynamoDB({})),
  tableName: 'my-table',
  logStatements: true, // Logs all interactions with Dynamo
}

export const myTableClient = TableClient.build(myTableDefinition, dynamoConfig);
