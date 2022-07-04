import { defineTable } from '../src';
import { defineTable as defineTable2 } from '../src/types';
import { exampleCarTable } from '../examples/example-table';

export const simpleTableDefinition = defineTable(
  {
    identifier: 'string',
    text: 'string',
  },
  'identifier',
);

export const complexTableDefinitionScan = defineTable(
  {
    hash: 'string',
    text: 'string?',
    obj: { optional: true, object: { abc: 'string', def: 'number?' } },
    arr: { optional: true, array: { object: { ghi: 'number?' } } },
    jkl: 'number?',
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
export const complexTableDefinition2 = defineTable2(
  {
    definition: {
      hash: 'string',
      text: 'string?',
      obj: {optional: true, object: {abc: 'string', def: 'number?'}},
      arr: {optional: true, array: {object: {ghi: 'number?'}}},
      jkl: 'number?',
    },
    partitionKey: 'hash'
  } as const
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

export const exampleCarTableDefinition = exampleCarTable;
