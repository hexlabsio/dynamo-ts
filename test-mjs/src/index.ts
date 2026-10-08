import { TableClient, TableDefinition } from '@hexlabs/dynamo-ts';

// Checks the package loads and works as a ES module

type Order = { customer: string; order: string; total: number };

const table = TableDefinition.ofType<Order>()
  .withPartitionKey('customer')
  .withSortKey('order')
  .withGlobalSecondaryIndex('by-total', 'customer')
  .withSortKey('total', 'number');

const shop = TableDefinition.singleTable(({ part, child }) => ({
  customer: part<{ store: string; customer: string; zone: string }>()
    .partitionedBy('store')
    .index('byZone', { partition: [], sort: ['zone'] })
    .with({ order: child<{ store: string; customer: string; order: string }>() }),
}));

const client = {} as never;
const orders = TableClient.build(table, { client, tableName: 'orders' });
const shopClient = shop.client({ client, tableName: 'shop' });

console.log(JSON.stringify(table.asCloudFormation('orders').AttributeDefinitions));
console.log(JSON.stringify(Object.keys(shop.asCloudFormation('shop'))));
console.log(typeof orders.query, typeof shopClient.customer.put, typeof shopClient.order.index);
