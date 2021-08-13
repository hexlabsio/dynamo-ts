import {DynamoDB} from "aws-sdk";
import {DynamoObjectDefinition, DynamoTable, TableEntryDefinition} from './dynamoTable';

export * from './dynamoTable';

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

const table = DynamoTable.build('', new DynamoDB.DocumentClient(), {definition: {a: 'string', b:'string'}, hashKey: 'a', rangeKey: 'b'});
table.query({a: '', b: sortKey => sortKey.between('x', 'y')})
