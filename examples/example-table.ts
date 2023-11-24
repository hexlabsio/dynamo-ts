import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { TableDefinition } from '../src/table-builder/table-definition';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/'  },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' }
});
export const exampleClient = DynamoDBDocument.from(dynamo);

export type Car = {
  make: string,
  identifier: string,
  model: string,
  year: number,
  colour: string
}

export const exampleCarTable = TableDefinition.ofType<Car>()
  .withPartitionKey('make')
  .withSortKey('identifier')
  .withGlobalSecondaryIndex('model-index', 'make').withSortKey('model')
  .withGlobalSecondaryIndex('model-year-index', 'model').withSortKey('year');
