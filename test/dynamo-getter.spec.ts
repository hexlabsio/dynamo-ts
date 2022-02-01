import {DynamoDB} from "aws-sdk";
import {TableClient} from "../src/table-client";
import {complexTableDefinition} from "./tables";

const dynamoClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
    sslEnabled: false,
    accessKeyId: 'xxxx',
    secretAccessKey: 'xxxx',
    region: 'local-env',
});

const testTable = TableClient.build(complexTableDefinition,{tableName: 'complexTableDefinition', client: dynamoClient, logStatements: true});

describe('Dynamo Table', () => {
    beforeAll(async () => {
       await testTable.put({hash: 'get-item-test', text: 'some text', obj: {abc: 'xyz', def: 2}});
    });

    describe('Get', () => {
        it('should get item and project to defined type', async () => {
          const result = await testTable.get({hash: 'get-item-test'}, {
              projection: projector => projector.project('obj.abc').project('text')
          });
          expect(result.item).toEqual({ text: 'some text', obj: {abc: 'xyz'}})
        });
    });
});
