import { TableDefinition } from '../src';

export type SimpleTable = {
  identifier: string;
  sort: string;
};

export type SimpleTable2 = SimpleTable & { text: string };
export type SimpleTable3 = { identifier: string; text: string };

export type ComplexTable2 = {
  hash: string;
  text: string;
  obj?: { abc: string; def?: number };
  arr?: { ghi?: number }[];
  jkl?: boolean;
  mno?: string | number;
  pqr: 'xxx' | 'yyy' | '123 456';
};

export type SetTable = {
  identifier: string;
  uniqueStrings: Set<string>;
  uniqueNumbers: Set<number>;
};

export type BinaryTable = {
  identifier: string;
  bin?: Buffer;
  binSet: Set<Buffer>;
};

export type NestedTable = {
  hash: string;
  string: string;
  stringOptional?: string;
  boolean: boolean;
  booleanOptional?: boolean;
  number: number;
  numberOptional?: number;
  nestedObject: { name: string };
  nestedObjectMultiple: { nestedObject: { name: string } };
  nestedObjectMultipleOptional?: { nestedObject: { name: string } };
  nestedObjectOptional?: { name: string };
  nestedObjectChildOptional: { name?: string };
  nestedObjectOptionalChildOptional?: { name?: string };
  arrayString: string[];
  arrayStringOptional?: string[];
  arrayObject: { name: string }[];
  arrayObjectOptional?: { name: string }[];
  nestedArrayString: { items: string[] };
  nestedArrayStringOptional?: { items: string[] };
  nestedArrayObject: { items: { name: string }[] };
  nestedArrayObjectOptional?: { items: { name: string }[] };
  mapType: Record<string, any>;
  listType: any[];
};

export type ComplexTable = {
  hash: string;
  text?: string;
  obj?: { abc: string; def?: number; qvc?: { a: string } };
  arr?: { ghi?: number }[];
  jkl?: number;
};

export type DeleteTable = {
  hash: string;
  text?: string;
  obj?: { abc: string; def: number };
  arr?: { ghi: string }[];
};

export type IndexTable = {
  hash: string;
  sort: string;
  indexHash: string;
};

export type TransactionTable = {
  identifier: string;
  count: number;
  description: string;
};
export const simpleTableDefinition =
  TableDefinition.ofType<SimpleTable>().withPartitionKey('identifier');
export const simpleTableDefinition2 = TableDefinition.ofType<SimpleTable2>()
  .withPartitionKey('identifier')
  .withSortKey('sort');
export const simpleTableDefinitionBatch =
  TableDefinition.ofType<SimpleTable3>().withPartitionKey('identifier');
export const simpleTableDefinitionBatch2 =
  TableDefinition.ofType<SimpleTable2>()
    .withPartitionKey('identifier')
    .withSortKey('sort');
export const simpleTableDefinition3 = TableDefinition.ofType<SimpleTable2>()
  .withPartitionKey('identifier')
  .withSortKey('sort');
export const transactionTableDefinition =
  TableDefinition.ofType<TransactionTable>().withPartitionKey('identifier');

export const complexTableDefinitionQuery =
  TableDefinition.ofType<ComplexTable2>()
    .withPartitionKey('hash')
    .withGlobalSecondaryIndex('abc', 'text')
    .withNoSortKey();

export const sortKeyAsIndexPartitionKeyTableDefinition =
  TableDefinition.ofType<ComplexTable2>()
    .withPartitionKey('hash')
    .withSortKey('text')
    .withGlobalSecondaryIndex('abc', 'text')
    .withNoSortKey();

export const setsTableDefinition =
  TableDefinition.ofType<SetTable>().withPartitionKey('identifier');

export const binaryTableDefinition =
  TableDefinition.ofType<BinaryTable>().withPartitionKey('identifier');

export const complexTableDefinitionFilter =
  TableDefinition.ofType<NestedTable>().withPartitionKey('hash');

export const complexTableDefinition =
  TableDefinition.ofType<ComplexTable>().withPartitionKey('hash');

export const deleteTableDefinition =
  TableDefinition.ofType<DeleteTable>().withPartitionKey('hash');

export const indexTableDefinition = TableDefinition.ofType<IndexTable>()
  .withPartitionKey('hash')
  .withSortKey('sort')
  .withGlobalSecondaryIndex('index', 'indexHash')
  .withSortKey('sort');

export const singleTableDesignDefinition = TableDefinition.ofType<{
  p2: string;
  s2: string;
}>()
  .withPartitionKey('p2')
  .withSortKey('s2');

export type Store = {
  org: string;
  store: string;
  zone: string;
  status?: 'open' | 'closed';
  name: string;
  visits?: number;
};

export type Employee = {
  org: string;
  store: string;
  employee: string;
  role: string;
};

export const storeSingleTable = TableDefinition.singleTable(
  { indexes: { byRole: { partitionKey: 'gsi2pk', sortKey: 'gsi2sk' } } },
  ({ part, child }) => ({
    store: part<Store>()
      .partitionedBy('org')
      .index('byZone', { partition: [], sort: ['zone', 'status'] })
      .with({
        employee: child<Employee>().index('byRole', {
          partition: ['role'],
          sort: ['employee'],
        }),
      }),
  }),
);

// The same schema in its own table, so update tests don't change the index test data
export const storeUpdatesSingleTable = storeSingleTable;

// And again for transaction tests
export const storeTransactionsSingleTable = storeSingleTable;

export type Reading = {
  sensor: string;
  time: number;
  site: string;
  value?: number;
};

// Number keys, to check the table and its indexes are created with number attributes
export const numberKeyTable = TableDefinition.ofType<Reading>()
  .withPartitionKey('sensor')
  .withSortKey('time', 'number')
  .withGlobalSecondaryIndex('by-value', 'site')
  .withSortKey('value', 'number');

export type Account = { tenant: string; account: string; name: string };

export type Invoice = {
  tenant: string;
  account: string;
  invoice: string;
  due: string;
  status?: 'open' | 'paid';
  amount: number;
};

// Local indexes, shared by accounts and the invoices that live in their partition
export const invoiceSingleTable = TableDefinition.singleTable(
  { indexes: { byStatus: { sortKey: 'lsi1' } } },
  ({ part, join }) => ({
    account: part<Account>()
      .partitionedBy('tenant')
      .localIndex('byName', { sort: ['name'] })
      .localIndex('byLabel', { sort: ['name'] })
      .with({
        invoice: join<Invoice>()
          .localIndex('byLabel', { sort: ['invoice'] })
          .localIndex('byDue', { sort: ['due'] })
          .localIndex('byStatus', { sort: ['status', 'due'] }),
      }),
  }),
);

// And again for the part batch, scan and paging tests
export const storeOperationsSingleTable = storeSingleTable;

export type Ticket = { project: string; ticket: number; title: string };

// A part keyed by a number
export const ticketSingleTable = TableDefinition.singleTable(({ part }) => ({
  ticket: part<Ticket>().partitionedBy('project'),
}));

// And again for select tests
export const invoiceSelectSingleTable = invoiceSingleTable;
