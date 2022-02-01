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

describe('Dynamo Putter', () => {
    describe('Simple Put', () => {
        it('should put item and return nothing', async () => {
          const result = await testTable.put({identifier: 'put-item-test', text: 'some text'});
          expect(result).toEqual(undefined);
          const getResult = await testTable.get({identifier: 'put-item-test'});
          expect(getResult.item).toEqual({identifier: 'put-item-test', text: 'some text'});
        });

        it('should return old value on second put', async () => {
            await testTable.put({identifier: 'put-item-test-2', text: 'test'});
            const result = await testTable.put({identifier: 'put-item-test-2', text: 'updated'}, {returnOldValues: true});
            expect(result).toEqual({identifier: 'put-item-test-2', text: 'test'});
        });
    });

    describe('Conditional Put', () => {

        it('should fail to put when identifier does not exist', async () => {
            await expect(testTable.put(
                {identifier: 'put-condition-test', text: 'some text'},
                {condition: compare => compare().exists('identifier')}
            )).rejects.toThrow(new Error("The conditional request failed"));
            const getResult = await testTable.get({identifier: 'put-condition-test'});
            expect(getResult.item).toEqual(undefined);
        });

        it('should fail to put when identifier exists', async () => {
            await testTable.put({identifier: 'put-condition-test-2', text: 'some text'});
            await expect(testTable.put(
                {identifier: 'put-condition-test-2', text: 'some text'},
                {condition: compare => compare().notExists('identifier')}
            )).rejects.toThrow(new Error("The conditional request failed"));
            const getResult = await testTable.get({identifier: 'put-condition-test-2'});
            expect(getResult.item).toEqual({identifier: 'put-condition-test-2', text: 'some text'});
        });

        it('should put when identifier exists and return old result', async () => {
            await testTable.put({identifier: 'put-condition-test-3', text: 'some text'});
            const result = await testTable.put(
                {identifier: 'put-condition-test-3', text: 'updated'},
                {
                    condition: compare => compare().exists('identifier'),
                    returnOldValues: true
                }
            )
            expect(result).toEqual({identifier: 'put-condition-test-3', text: 'some text'});
            const getResult = await testTable.get({identifier: 'put-condition-test-3'});
            expect(getResult.item).toEqual({identifier: 'put-condition-test-3', text: 'updated'});
        });
    });
});