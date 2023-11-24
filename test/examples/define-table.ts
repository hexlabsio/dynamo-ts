import { TableDefinition } from '../../src/table-builder/table-definition';

type MyTableType = { identifier: string; sort: string; abc: { xyz: number } };

export const myTableDefinition = TableDefinition.ofType<MyTableType>()
  .withPartitionKey('identifier') // <- type checked to be a key in your type
  .withSortKey('sort') // <- optional, aso type checked
  .withGlobalSecondaryIndex('my-index', 'sort').withNoSortKey() // Global or Local index
