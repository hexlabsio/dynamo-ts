import {DynamoDB} from "aws-sdk";
import { TableClient} from "../src";
import {complexTableDefinition} from "./tables";

const dynamoClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
    sslEnabled: false,
    accessKeyId: 'xxxx',
    secretAccessKey: 'xxxx',
    region: 'local-env',
});


const testTable = TableClient.build(complexTableDefinition,{tableName: 'complexTableDefinition', client: dynamoClient, logStatements: true});

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