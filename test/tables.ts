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
    arrx: { array: "string" } ,
    jkl: 'boolean?',
    mno: 'string | number | undefined',
    pqr: '"xxx" | "yyy" | "123 456"',
  },
  'hash',
  null,
  { abc: { partitionKey: 'text' } },
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
