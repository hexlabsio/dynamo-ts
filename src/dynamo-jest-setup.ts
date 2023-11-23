import * as fs from 'fs';
import { TableDefinition } from './table-builder/table-definition';

export function dynamoTable(
  definition: TableDefinition,
  name?: string,
  props?: any,
): any {
  const indexKeys = Object.keys(definition.indexes ?? {}).flatMap((key) => [
    (definition.indexes as any)![key].partitionKey as string,
    ...((definition.indexes as any)![key].sortKey
      ? [(definition.indexes as any)![key].sortKey! as string]
      : []),
  ]);
  const keys: string[] = [
    ...new Set([
      definition.keyNames.partitionKey as string,
      ...(definition.keyNames.sortKey ? [definition.keyNames.sortKey! as string] : []),
      ...indexKeys,
    ]),
  ];

  return {
    ...props,
    ...(name ? { TableName: name } : {}),
    KeySchema: [
      { KeyType: 'HASH', AttributeName: definition.keyNames.partitionKey as string },
      ...(definition.keyNames.sortKey
        ? [{ KeyType: 'RANGE', AttributeName: definition.keyNames.sortKey as string }]
        : []),
    ],
    AttributeDefinitions: keys.map((key) => ({
      AttributeName: key as string,
      AttributeType: 'S',
    })),
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
    ...(definition.indexes && Object.keys(definition.indexes as any).length > 0
      ? {
          GlobalSecondaryIndexes: Object.keys(definition.indexes as any).filter(index => (definition.indexes as any)[index]!.global).map(
            (key) => {
              return {
                IndexName: key,
                KeySchema: [
                  {
                    KeyType: 'HASH',
                    AttributeName: (definition.indexes as any)[key]
                      .partitionKey as string,
                  },
                  ...((definition.indexes as any)[key].sortKey
                    ? [
                        {
                          KeyType: 'RANGE',
                          AttributeName: (definition.indexes as any)![key]
                            .sortKey as string,
                        },
                      ]
                    : []),
                ],
                ProvisionedThroughput: {
                  ReadCapacityUnits: 1,
                  WriteCapacityUnits: 1,
                },
                Projection: { ProjectionType: 'ALL' },
              };
            },
          ),
        }
      : {}),
  };
}

export function tableDefinition(definitions: Record<string, TableDefinition>): {
  tables: unknown[];
} {
  return {
    tables: Object.keys(definitions).map((table) =>
      dynamoTable(definitions[table], table),
    ),
  };
}

export function writeJestDynamoConfig(
  definitions: Record<string, TableDefinition>,
  name = 'jest-dynamodb-config.js',
  rest = {},
): void {
  const definition = tableDefinition(definitions);
  fs.writeFileSync(
    name,
    `module.exports = ${JSON.stringify({ ...rest, ...definition }, null, 2)};`,
  );
}
