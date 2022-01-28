import {TableEntryDefinition} from './dynamoTable';
import {DynamoObjectDefinition} from "./type-mapping";

export * from './dynamoTable';
export * from './type-mapping';
export * from './dynamoIndex';
export * from './operation';
export * from './query';

export function defineTable<
  D extends DynamoObjectDefinition['object'],
  H extends keyof D,
  R extends keyof D | null = null,
  G extends Record<
    string,
    { hashKey: keyof D; rangeKey?: keyof D }
  > | null = null,
>(
  definition: TableEntryDefinition<D, H, R, G>,
): TableEntryDefinition<D, H, R, G> {
  return definition;
}
