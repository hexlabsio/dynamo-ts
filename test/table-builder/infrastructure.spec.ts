import { TableDefinition } from '../../src';

type Order = {
  customer: string;
  order: string;
  status: string;
  created: string;
  total: number;
};

const orderTable = TableDefinition.ofType<Order>()
  .withPartitionKey('customer')
  .withSortKey('order')
  .withGlobalSecondaryIndex('status-index', 'status')
  .withSortKey('created')
  .withLocalSecondaryIndex('created-index')
  .withSortKey('created');

const simpleTable = TableDefinition.ofType<{
  id: string;
  text: string;
}>().withPartitionKey('id');

// Stand-in for the aws-cdk-lib/aws-dynamodb module
enum AttributeType {
  STRING = 'S',
  NUMBER = 'N',
  BINARY = 'B',
}
enum ProjectionType {
  ALL = 'ALL',
  KEYS_ONLY = 'KEYS_ONLY',
}
const dynamodb = { AttributeType, ProjectionType };

describe('Infrastructure output', () => {
  describe('Terraform', () => {
    it('should describe keys, attributes and indexes', () => {
      expect(
        orderTable.asTerraform('orders', { billing_mode: 'PAY_PER_REQUEST' }),
      ).toEqual({
        name: 'orders',
        billing_mode: 'PAY_PER_REQUEST',
        hash_key: 'customer',
        range_key: 'order',
        attribute: [
          { name: 'customer', type: 'S' },
          { name: 'order', type: 'S' },
          { name: 'status', type: 'S' },
          { name: 'created', type: 'S' },
        ],
        global_secondary_index: [
          {
            name: 'status-index',
            key_schema: [
              { attribute_name: 'status', key_type: 'HASH' },
              { attribute_name: 'created', key_type: 'RANGE' },
            ],
            projection_type: 'ALL',
          },
        ],
        local_secondary_index: [
          {
            name: 'created-index',
            range_key: 'created',
            projection_type: 'ALL',
          },
        ],
      });
    });

    it('should copy provisioned capacity to global indexes', () => {
      const result = orderTable.asTerraform('orders', {
        billing_mode: 'PROVISIONED',
        read_capacity: 5,
        write_capacity: 2,
      });
      expect(result.read_capacity).toEqual(5);
      expect(result.global_secondary_index![0]).toEqual(
        expect.objectContaining({ read_capacity: 5, write_capacity: 2 }),
      );
    });

    it('should leave out sort key and indexes when there are none', () => {
      expect(simpleTable.asTerraform('simple')).toEqual({
        name: 'simple',
        hash_key: 'id',
        attribute: [{ name: 'id', type: 'S' }],
      });
    });

    it('should write HCL', () => {
      const hcl = orderTable.asTerraformHcl('orders', 'orders-${env}', {
        billing_mode: 'PAY_PER_REQUEST',
        tags: { team: 'shop', 'cost-centre': 'abc' },
        point_in_time_recovery: { enabled: true },
      });
      expect(hcl).toEqual(`resource "aws_dynamodb_table" "orders" {
  name = "orders-$\${env}"
  billing_mode = "PAY_PER_REQUEST"
  tags = {
    team = "shop"
    cost-centre = "abc"
  }
  point_in_time_recovery {
    enabled = true
  }
  hash_key = "customer"
  range_key = "order"
  attribute {
    name = "customer"
    type = "S"
  }
  attribute {
    name = "order"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }
  attribute {
    name = "created"
    type = "S"
  }
  global_secondary_index {
    name = "status-index"
    key_schema {
      attribute_name = "status"
      key_type = "HASH"
    }
    key_schema {
      attribute_name = "created"
      key_type = "RANGE"
    }
    projection_type = "ALL"
  }
  local_secondary_index {
    name = "created-index"
    range_key = "created"
    projection_type = "ALL"
  }
}
`);
    });
  });

  describe('CDK', () => {
    it('should describe TableV2 props using the CDK enums', () => {
      const props = orderTable.asCdk(dynamodb, 'orders', {
        deletionProtection: true,
      });
      const partitionType: AttributeType = props.partitionKey.type;
      expect(partitionType).toEqual(AttributeType.STRING);
      expect(props).toEqual({
        tableName: 'orders',
        deletionProtection: true,
        partitionKey: { name: 'customer', type: 'S' },
        sortKey: { name: 'order', type: 'S' },
        globalSecondaryIndexes: [
          {
            indexName: 'status-index',
            partitionKey: { name: 'status', type: 'S' },
            sortKey: { name: 'created', type: 'S' },
            projectionType: 'ALL',
          },
        ],
        localSecondaryIndexes: [
          {
            indexName: 'created-index',
            sortKey: { name: 'created', type: 'S' },
            projectionType: 'ALL',
          },
        ],
      });
    });

    it('should leave out the table name when not given', () => {
      expect(simpleTable.asCdk(dynamodb)).toEqual({
        partitionKey: { name: 'id', type: 'S' },
      });
    });
  });

  describe('SST', () => {
    it('should describe Dynamo args', () => {
      expect(orderTable.asSst({ stream: 'new-and-old-images' })).toEqual({
        stream: 'new-and-old-images',
        fields: {
          customer: 'string',
          order: 'string',
          status: 'string',
          created: 'string',
        },
        primaryIndex: { hashKey: 'customer', rangeKey: 'order' },
        globalIndexes: {
          'status-index': {
            hashKey: 'status',
            rangeKey: 'created',
            projection: 'all',
          },
        },
        localIndexes: {
          'created-index': { rangeKey: 'created', projection: 'all' },
        },
      });
    });

    it('should leave out sort key and indexes when there are none', () => {
      expect(simpleTable.asSst()).toEqual({
        fields: { id: 'string' },
        primaryIndex: { hashKey: 'id' },
      });
    });
  });

  it('should only allow local indexes with a sort key on tables with a sort key', () => {
    const base = TableDefinition.ofType<{
      a: string;
      b: string;
      c: number;
    }>().withPartitionKey('a');
    // @ts-expect-error local indexes need a table with a sort key
    expect(() => base.withLocalSecondaryIndex('broken')).toThrow(
      'Local secondary index broken needs a table with a sort key',
    );
    const local = base.withSortKey('b').withLocalSecondaryIndex('by-c');
    // @ts-expect-error local indexes always have a sort key
    expect(local.withNoSortKey).toBeUndefined();
    expect(local.withSortKey('c', 'number').indexes).toEqual({
      'by-c': { global: false, partitionKey: 'a', sortKey: 'c' },
    });
  });
});
