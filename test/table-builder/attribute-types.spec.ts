import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient, TableDefinition } from '../../src';
import { numberKeyTable } from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const client = TableClient.build(numberKeyTable, {
  client: DynamoDBDocument.from(dynamo),
  tableName: 'numberKeyTable',
});

type Item = {
  id: string;
  version: number;
  data: Buffer;
  label: string;
  maybe?: string;
  either: string | number;
  nested: { a: string };
};

// Stand-in for the aws-cdk-lib/aws-dynamodb module
enum AttributeType {
  STRING = 'S',
  NUMBER = 'N',
  BINARY = 'B',
}
enum ProjectionType {
  ALL = 'ALL',
}
const dynamodb = { AttributeType, ProjectionType };

const table = TableDefinition.ofType<Item>()
  .withPartitionKey('id')
  .withSortKey('version', 'number')
  .withGlobalSecondaryIndex('by-data', 'data', 'binary')
  .withSortKey('label')
  .withGlobalSecondaryIndex('by-maybe', 'maybe')
  .withNoSortKey();

describe('Key attribute types', () => {
  it('should declare number and binary keys in CloudFormation', () => {
    expect(table.asCloudFormation('items').AttributeDefinitions).toEqual([
      { AttributeName: 'id', AttributeType: 'S' },
      { AttributeName: 'version', AttributeType: 'N' },
      { AttributeName: 'data', AttributeType: 'B' },
      { AttributeName: 'label', AttributeType: 'S' },
      { AttributeName: 'maybe', AttributeType: 'S' },
    ]);
  });

  it('should declare number and binary keys in Terraform', () => {
    expect(table.asTerraform('items').attribute).toEqual([
      { name: 'id', type: 'S' },
      { name: 'version', type: 'N' },
      { name: 'data', type: 'B' },
      { name: 'label', type: 'S' },
      { name: 'maybe', type: 'S' },
    ]);
    expect(table.asTerraformHcl('items', 'items')).toContain(
      'attribute {\n    name = "version"\n    type = "N"\n  }',
    );
  });

  it('should declare number and binary keys in CDK', () => {
    const props = table.asCdk(dynamodb);
    expect(props.sortKey).toEqual({
      name: 'version',
      type: AttributeType.NUMBER,
    });
    expect(props.globalSecondaryIndexes![0].partitionKey).toEqual({
      name: 'data',
      type: AttributeType.BINARY,
    });
  });

  it('should declare number and binary keys in SST', () => {
    expect(table.asSst().fields).toEqual({
      id: 'string',
      version: 'number',
      data: 'binary',
      label: 'string',
      maybe: 'string',
    });
  });

  it('should keep attribute types when querying an index', () => {
    expect(table.asIndex('by-data').attributeTypes).toEqual({
      id: 'S',
      version: 'N',
      data: 'B',
      label: 'S',
      maybe: 'S',
    });
  });

  it('should require the type of keys that are not strings', () => {
    const builder = TableDefinition.ofType<Item>();
    // @ts-expect-error version is a number, so its type must be given
    builder.withPartitionKey('version');
    // @ts-expect-error version is a number, not a string
    builder.withPartitionKey('version', 'string');
    // @ts-expect-error label is a string, not a number
    builder.withPartitionKey('label', 'number');
    builder.withPartitionKey('label', 'string');
    // @ts-expect-error data is binary, so its type must be given
    builder.withPartitionKey('id').withGlobalSecondaryIndex('x', 'data');
    builder
      .withPartitionKey('id')
      // @ts-expect-error either could be a string or a number, so it can't be a key
      .withGlobalSecondaryIndex('x', 'either', 'string');
    builder
      .withPartitionKey('id')
      // @ts-expect-error nested is an object, so it can't be a key
      .withGlobalSecondaryIndex('x', 'nested', 'string');
    builder
      .withPartitionKey('id')
      .withGlobalSecondaryIndex('x', 'label')
      // @ts-expect-error version is a number, so its type must be given
      .withSortKey('version');
  });

  it('should create and query tables with number keys', async () => {
    await client
      .batchPut([
        { sensor: 'a', time: 9, site: 'north', value: 3 },
        { sensor: 'a', time: 10, site: 'north', value: 20 },
        { sensor: 'a', time: 100, site: 'south', value: 1 },
        { sensor: 'a', time: 50, site: 'north' },
      ])
      .execute();
    const recent = await client.query({
      sensor: 'a',
      time: (time) => time.between(9, 50),
    });
    // Number keys sort numerically, not as text
    expect(recent.member.map((it) => it.time)).toEqual([9, 10, 50]);
    const north = await client
      .index('by-value')
      .query({ site: 'north', value: (value) => value.gt(2) });
    expect(north.member.map((it) => it.value)).toEqual([3, 20]);
  });
});
