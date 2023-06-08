import {
  DynamoEntry,
  DynamoIndexBaseKeys,
  DynamoIndexes,
  DynamoMapDefinition,
} from './type-mapping';

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
