import {DynamoDB} from "aws-sdk";
import {defineTable, TableClient} from "../src";

const dynamoClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
    sslEnabled: false,
    region: 'local-env',
});

const tableDefinition = defineTable({
    hash: 'string', text: 'string?', obj: { optional: true, object: {abc: 'string'}}, arr: { optional: true, array: {object: {ghi: 'string'}}}
}, 'hash');

const testTable = TableClient.build(tableDefinition,{tableName: 'test-scan-table', client: dynamoClient, logStatements: true});

describe('Dynamo Scanner', () => {
    describe('Simple Scanner', () => {
        it('should put item and return nothing', async () => {
          await testTable.put({hash: 'scan-item-test', text: 'some text', obj: {abc: 'def'}});
          await testTable.put({hash: 'scan-item-test2', text: 'some text', arr: []});
          await testTable.put({hash: 'scan-item-test3', text: 'some text', arr: [{ghi: 'a'}, {ghi: 'b'}]});
          await testTable.put({hash: 'scan-item-test4', text: 'some text', arr: [{ghi: 'a'}]});
          await testTable.put({hash: 'scan-item-test5'});
          const result = await testTable.scan({filter: compare => compare().existsPath('arr[1]')});
          expect(result.member).toEqual([{hash: 'scan-item-test3', text: 'some text', arr: [{ghi: 'a'}, {ghi: 'b'}]}]);
        });
    });
});