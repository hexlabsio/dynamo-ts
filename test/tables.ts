import { TableDefinition } from '../src/table-builder/table-definition';

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
  obj?: { abc: string; def?: number };
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
  partition: string;
  sort: string;
}>()
  .withPartitionKey('partition')
  .withSortKey('sort');
