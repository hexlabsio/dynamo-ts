import { defineTable } from '../src';
import { exampleCarTable } from '../examples/example-table';

export const simpleTableDefinition = defineTable(
  {
    identifier: 'string',
    text: 'string',
  },
  'identifier',
);

export const complexTableDefinition = defineTable(
  {
    hash: 'string',
    text: 'string?',
    obj: { optional: true, object: { abc: 'string', def: 'number' } },
    arr: { optional: true, array: { object: { ghi: 'string' } } },
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

export const exampleCarTableDefinition = exampleCarTable;
