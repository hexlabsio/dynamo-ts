import * as fs from 'fs';
import { TableDefinition } from './table-builder/table-definition.js';

export function tableDefinition(definitions: Record<string, TableDefinition>): {
  tables: unknown[];
} {
  return {
    tables: Object.keys(definitions).map((table) =>
      definitions[table].asCloudFormation(table, {
        ProvisionedThroughput: { WriteCapacityUnits: 1, ReadCapacityUnits: 1 },
      }),
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
