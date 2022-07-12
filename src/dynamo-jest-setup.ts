import * as fs from 'fs';
import { Definition, DynamoDefinition } from './types';

type DynamoDetails = {
  definition: DynamoDefinition,
  partitionKey: string,
  sortKey?: string | null,
  indexes: Record<string, {partitionKey: string, sortKey?: string | null}>
}

export function dynamoTable(
  definition: DynamoDetails,
  name?: string,
  props?: any,
): any {
  const indexKeys = Object.keys(definition.indexes ?? {}).flatMap((key) => [
    definition.indexes![key].sortKey as string,
    ...(definition.indexes![key].sortKey
      ? [definition.indexes![key].sortKey! as string]
      : []),
  ]);
  const keys: string[] = [
    ...new Set([
      definition.partitionKey as string,
      ...(definition.sortKey ? [definition.sortKey! as string] : []),
      ...indexKeys,
    ]),
  ];
  function typeFor(key: string): string {
    const type: Definition = definition.definition[key];
    const nonOptionalType = `${type}`.endsWith('?')
      ? type.toString().substring(0, type.toString().length - 1)
      : type;
    switch (nonOptionalType) {
      case 'string':
        return 'S';
      case 'string set':
        return 'SS';
      case 'number':
        return 'N';
      case 'number set':
        return 'NS';
      case 'binary':
        return 'B';
      case 'binary set':
        return 'BS';
      case 'boolean':
        return 'BOOL';
      case 'null':
        return 'NULL';
      case 'list':
        return 'L';
      default:
        return 'M';
    }
  }
  return {
    ...props,
    ...(name ? { TableName: name } : {}),
    KeySchema: [
      { KeyType: 'HASH', AttributeName: definition.partitionKey as string },
      ...(definition.sortKey
        ? [{ KeyType: 'RANGE', AttributeName: definition.sortKey as string }]
        : []),
    ],
    AttributeDefinitions: keys.map((key) => ({
      AttributeName: key as string,
      AttributeType: typeFor(key),
    })),
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
    ...(definition.indexes && Object.keys(definition.indexes as any).length > 0
      ? {
          GlobalSecondaryIndexes: Object.keys(definition.indexes as any).map(
            (key) => {
              return {
                IndexName: key,
                KeySchema: [
                  {
                    KeyType: 'HASH',
                    AttributeName: definition.indexes![key].partitionKey as string,
                  },
                  ...(definition.indexes![key].sortKey
                    ? [
                        {
                          KeyType: 'RANGE',
                          AttributeName: definition.indexes![key]
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

export function tableDefinition(
  definitions: Record<string, DynamoDetails>,
): { tables: unknown[] } {
  return {
    tables: Object.keys(definitions).map((table) =>
      dynamoTable(definitions[table], table),
    ),
  };
}

export function writeJestDynamoConfig(
  definitions: Record<string, DynamoDetails>,
  name = 'jest-dynamodb-config.js',
  rest = {},
): void {
  const definition = tableDefinition(definitions);
  fs.writeFileSync(
    name,
    `module.exports = ${JSON.stringify({ ...rest, ...definition }, null, 2)};`,
  );
}
