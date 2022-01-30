import {TableEntryDefinition} from './dynamoTable';
import {DynamoEntry, DynamoMapDefinition, DynamoObjectDefinition, DynamoRangeKey} from "./type-mapping";
import {TableClient} from "./table-client";
import {DynamoDefinition} from "./dynamo-client-config";

export * from './dynamoTable';
export * from './type-mapping';
export * from './dynamoIndex';
export * from './operation';
export * from './query';

export function defineTable2<
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
