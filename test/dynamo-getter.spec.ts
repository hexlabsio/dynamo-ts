import {DynamoDB} from "aws-sdk";
import {defineTable, TableClient} from "../src/table-client";

const dynamoClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
    sslEnabled: false,
    region: 'local-env',
});

const tableDefinition = defineTable({
    identifier: 'string', text: 'string'
}, 'identifier');

const testTable = TableClient.build(tableDefinition,{tableName: 'test-get-table', client: dynamoClient, logStatements: true});

describe('Dynamo Table', () => {
    beforeAll(async () => {
       await testTable.put({identifier: 'get-item-test', text: 'some text', other: 'something else'} as any);
    });

    describe('Get', () => {
        it('should get item and project to defined type', async () => {
          const result = await testTable.get({identifier: 'get-item-test'});
          expect(result).toEqual({identifier: 'get-item-test', text: 'some text'})
        });

        it('should override projection if supplied', async () => {
            const result = await testTable.get({identifier: 'get-item-test'}, {ProjectionExpression: '#other', ExpressionAttributeNames: { '#other': 'other'}});
            expect(result).toEqual({other: 'something else'})
        });
    });
});
