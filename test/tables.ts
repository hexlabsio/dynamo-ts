import {defineTable} from "../src";

export const simpleTableDefinition = defineTable({
    identifier: 'string', text: 'string'
}, 'identifier');

export const complexTableDefinition = defineTable({
    hash: 'string', text: 'string?', obj: { optional: true, object: {abc: 'string', def: 'number'}}, arr: { optional: true, array: {object: {ghi: 'string'}}}
}, 'hash');