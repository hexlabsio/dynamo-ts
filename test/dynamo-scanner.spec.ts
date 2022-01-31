import {DynamoDB} from "aws-sdk";
import {defineTable, TableClient} from "../src/table-client";

const dynamoClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
    sslEnabled: false,
    region: 'local-env',
});

const tableDefinition = defineTable({
    hash: 'string', text: 'string?'
}, 'hash');

const testTable = TableClient.build(tableDefinition,{tableName: 'test-scan-table', client: dynamoClient, logStatements: true});

describe('Dynamo Scanner', () => {
    describe('Simple Scanner', () => {
        it('should put item and return nothing', async () => {
          await testTable.put({hash: 'scan-item-test', text: 'some text'});
          await testTable.put({hash: 'scan-item-test-2'});
          const result = await testTable.scan({filter: compare => compare().exists('text')});
          expect(result.member).toEqual([{hash: 'scan-item-test', text: 'some text'}]);
        });
    });
});