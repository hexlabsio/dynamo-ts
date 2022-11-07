import { defineTable } from '../src';

export const simpleTableDefinition = defineTable(
  {
    identifier: 'string',
    text: 'string',
  },
  'identifier',
);

export const simpleTableDefinition2 = defineTable(
  {
    identifier: 'string',
    sort: 'string',
    text: 'string',
  },
  'identifier',
  'sort',
);

export const simpleTableDefinition3 = defineTable(
  {
    identifier: 'string',
    sort: 'string',
    text: 'string',
  },
  'identifier',
  'sort',
);

export const complexTableDefinitionQuery = defineTable(
  {
    hash: 'string',
    text: 'string?',
    obj: { optional: true, object: { abc: 'string', def: 'number?' } },
    arr: { optional: true, array: { object: { ghi: 'number?' } } },
    jkl: 'boolean?',
    mno: 'string | number | undefined',
    pqr: '"xxx" | "yyy" | "123 456"',
  },
  'hash',
  null,
  { abc: { partitionKey: 'text' } },
);

export const complexTableDefinitionFilter = defineTable(
  {
    hash: 'string',
    string: 'string',
    stringOptional: 'string?',
    boolean: 'boolean',
    booleanOptional: 'boolean?',
    number: 'number',
    numberOptional: 'number?',
    nestedObject: { object: { name: 'string' } },
    nestedObjectMultiple: { object: { nestedObject: { object: { name: 'string' } } } },
    nestedObjectMultipleOptional: { optional: true, object: { nestedObject: { object: { name: 'string' } } } },
    nestedObjectOptional: { optional: true, object: { name: 'string' } },
    nestedObjectChildOptional: { object: { name: 'string?' } },
    nestedObjectOptionalChildOptional: { optional: true, object: { name: 'string?' } },
    arrayString: { array: "string" },
    arrayStringOptional: { optional: true, array: "string" },
    arrayObject: { array: { object: { name: 'string' } } },
    arrayObjectOptional: { optional: true, array: { object: { name: 'string' } } },
    nestedArrayString: { object: { items: { array: "string" } } },
    nestedArrayStringOptional: { optional: true, object: { items: { array: "string" } } },
    nestedArrayObject: { object: { items: { array: { object: { name: 'string' } } } } },
    nestedArrayObjectOptional: { optional: true, object: { items: { array: { object: { name: 'string' } } } } },
    mapType: "map",
    listType: "list"
  },
  'hash',
);

export const complexTableDefinition = defineTable(
  {
    hash: 'string',
    text: 'string?',
    obj: { optional: true, object: { abc: 'string', def: 'number?' } },
    arr: { optional: true, array: { object: { ghi: 'number?' } } },
    jkl: 'number?',
  },
  'hash',
);

export const deleteTableDefinition = defineTable(
  {
    hash: 'string',
    text: 'string?',
    obj: { optional: true, object: { abc: 'string', def: 'number' } },
    arr: { optional: true, array: { object: { ghi: 'string' } } },
  },
  'hash',
);

export const indexTableDefinition = defineTable(
  {
      hash: 'string',
      sort: 'string',
      indexHash: 'string',
  },
  'hash',
  'sort',
  {
      'index': { partitionKey: 'indexHash', sortKey: 'sort' }
  }
);
