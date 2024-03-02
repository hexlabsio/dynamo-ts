import * as tables from './tables.js';
import { writeJestDynamoConfig } from '../src/dynamo-jest-setup.js';

(async () =>
  writeJestDynamoConfig(tables, 'dynamodb-tables.cjs', { port: 5001 }))();
