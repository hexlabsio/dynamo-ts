import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

export interface DynamoConfig {
  logStatements?: boolean;
  tableName: string;
  indexName?: string;
  client: DynamoDBDocument;
}
