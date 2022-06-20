import {
  DynamoEntry,
  DynamoIndexBaseKeys,
  DynamoIndexes,
  DynamoMapDefinition,
} from './type-mapping';
import { DynamoDB } from 'aws-sdk';

export interface DynamoClientConfig<DEFINITION extends DynamoMapDefinition> {
  definition: DEFINITION;
  tableType: DynamoEntry<DEFINITION>;
  tableName: string;
  indexName?: string;
  logStatements?: boolean;
  client: DynamoDB.DocumentClient;
}

export type DynamoDefinition<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends Omit<keyof DynamoEntry<DEFINITION>, HASH> | null,
  INDEXES extends DynamoIndexes<DEFINITION> = null,
  BASEKEYS extends DynamoIndexBaseKeys<DEFINITION> = null,
> = {
  definition: DEFINITION;
  hash: HASH;
  range: RANGE;
  indexes: INDEXES;
  baseKeys: BASEKEYS;
};
