import * as fs from 'fs';
import { DynamoDefinition } from './dynamo-client-config';
import { DynamoType } from './type-mapping';

export function dynamoTable(
  definition: DynamoDefinition<any, any, any, any>,
  name?: string,
  props?: any,
): any {
  const indexKeys = Object.keys(definition.indexes ?? {}).flatMap((key) => [
    definition.indexes![key].hashKey as string,
    ...(definition.indexes![key].rangeKey
      ? [definition.indexes![key].rangeKey! as string]
      : []),
  ]);
  const keys: string[] = [
    ...new Set([
      definition.hash as string,
      ...(definition.range ? [definition.range! as string] : []),
      ...indexKeys,
    ]),
  ];
  function typeFor(key: string): string {
    const type: DynamoType = definition.definition[key];
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
      { KeyType: 'HASH', AttributeName: definition.hash as string },
      ...(definition.range
        ? [{ KeyType: 'RANGE', AttributeName: definition.range as string }]
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
                    AttributeName: definition.indexes![key].hashKey as string,
                  },
                  ...(definition.indexes![key].rangeKey
                    ? [
                        {
                          KeyType: 'RANGE',
                          AttributeName: definition.indexes![key]
                            .rangeKey as string,
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
  defintions: Record<string, DynamoDefinition<any, any, any, any>>,
): { tables: unknown[] } {
  return {
    tables: Object.keys(defintions).map((table) =>
      dynamoTable(defintions[table], table),
    ),
  };
}

export function writeJestDynamoConfig(
  defintions: Record<string, DynamoDefinition<any, any, any, any>>,
  name = 'jest-dynamodb-config.js',
): void {
  const definition = tableDefinition(defintions);
  fs.writeFileSync(
    name,
    `module.exports = ${JSON.stringify(definition, null, 2)};`,
  );
}
