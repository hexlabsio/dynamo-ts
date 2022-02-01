import {DynamoDB} from "aws-sdk";
import {TableClient} from "../src/table-client";
import {simpleTableDefinition} from "./tables";

const dynamoClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
    sslEnabled: false,
    accessKeyId: 'xxxx',
    secretAccessKey: 'xxxx',
    region: 'local-env',
});

const testTable = TableClient.build(simpleTableDefinition,{tableName: 'simpleTableDefinition', client: dynamoClient, logStatements: true});

describe('Dynamo Table', () => {
    beforeAll(async () => {
       await testTable.put({identifier: 'get-item-test', text: 'some text', other: 'something else'} as any);
    });

    describe('Get', () => {
        it('should get item and project to defined type', async () => {
          const result = await testTable.get({identifier: 'get-item-test'});
          expect(result.item).toEqual({identifier: 'get-item-test', text: 'some text'})
        });

        it('should override projection if supplied', async () => {
            const result = await testTable.get({identifier: 'get-item-test'}, {ProjectionExpression: '#other', ExpressionAttributeNames: { '#other': 'other'}});
            expect(result.item).toEqual({other: 'something else'})
        });
    });
});
