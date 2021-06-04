
import { ddbMock, expectAttributeValueKV, mockDDBquery } from "./testUtil";
import '../src/extensions';
import { DocumentClient } from "aws-sdk/clients/dynamodb";


type Foo = {
    bar: string,
    baz: number,
    bop: string;
};

describe('client query', () => {

    it('should accept key params', async () => {
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table', { key: 'bar', comparison: ['begins_with', 'a'] });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ExpressionAttributeNames!).toEqual({ '#bar': 'bar' });
        const [eqKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, 'a');
        expect(capturedParam.KeyConditionExpression).toEqual(`begins_with(#bar, ${eqKey})`);
    });

    it('should accept projection options', async () => {
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            { projection: ['bop', 'baz'] });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ExpressionAttributeNames!).toEqual({ '#bar': 'bar', '#bop': 'bop', '#baz': 'baz' });
        expect(capturedParam.ProjectionExpression!).toBe('#bop, #baz');
    });

    it('should accept filter options', async () => {
        const eqLookup = 123;
        const beginsWithLookup = 'aaa';
        const [lowerRange, upperRange] = [0, 100];
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            {
                filters: {
                    $or:
                        [{
                            $and: [
                                { key: 'bar', comparison: ['=', eqLookup] },
                                { key: 'baz', comparison: ['between', lowerRange, upperRange] },
                            ]
                        },
                        { key: 'bop', comparison: ['begins_with', beginsWithLookup] }
                        ]
                }
            });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ExpressionAttributeNames!).toEqual({ '#bar': 'bar', '#bop': 'bop', '#baz': 'baz' });
        expect(capturedParam.ExpressionAttributeValues).toBeDefined();
        const [eqKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, eqLookup);
        const [beginsKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, beginsWithLookup);

        const [lowerRangeKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, upperRange);

        expect(capturedParam.FilterExpression).toEqual(`((#bar = ${eqKey}) AND (#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey})) OR (begins_with(#bop, ${beginsKey}))`);

    });

    it('should map index name', async () => {
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            { index: 'Foo-IDX' });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.IndexName!).toEqual('Foo-IDX');
    });

    it('should map sort asc', async () => {
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            { sort: 'asc' });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ScanIndexForward!).toBe(true);
    });

    it('should map sort desc', async () => {
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            { sort: 'desc' });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ScanIndexForward!).toBe(false);
    });

    it('should map offsetKey', async () => {
        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            { offsetKey: { baz: 1, bar: 'z' } });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ExclusiveStartKey!).toStrictEqual({ baz: 1, bar: 'z' });
    });

    it('should accept multiple options', async () => {
        const eqLookup = 123;
        const beginsWithLookup = 'aaa';
        const [lowerRange, upperRange] = [0, 100];

        const queryMockFn = jest.fn();
        const documentClient = ddbMock(queryMockFn);
        const docQuerySpy = jest.spyOn(documentClient, "query").mockImplementation(mockDDBquery);

        await documentClient.queryExtra<Foo>('table',
            { key: 'bar', comparison: ['begins_with', 'a'] },
            {
                projection: ['bop', 'baz'],
                filters: {
                    $or:
                        [{
                            $and: [
                                { key: 'bop', comparison: 'attribute_exists' },
                                { key: 'bar', comparison: ['=', eqLookup] },
                                { key: 'baz', comparison: ['between', lowerRange, upperRange] },
                            ]
                        },
                        { key: 'bop', comparison: ['begins_with', beginsWithLookup] }
                        ]
                },
                index: 'Foo-IDX',
                sort: 'desc',
                limit: 10,
                offsetKey: { baz: 1, bar: 'z' }
            });

        const capturedParam = captureParamAs<DocumentClient.QueryInput>(docQuerySpy, queryMockFn);

        expect(capturedParam.ExclusiveStartKey!).toStrictEqual({ baz: 1, bar: 'z' });
        expect(capturedParam.ExpressionAttributeNames!).toStrictEqual({ "#bar": "bar", "#baz": "baz", "#bop": "bop" });
        expect(capturedParam.Limit!).toBe(10);
        expect(capturedParam.IndexName!).toBe('Foo-IDX');
        expect(capturedParam.ScanIndexForward!).toBe(false);
        expect(capturedParam.ProjectionExpression!).toBe('#bop, #baz');


        expect(capturedParam.ExpressionAttributeValues).toBeDefined();
        // key expression assertions
        const [eqKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, 'a');
        expect(capturedParam.KeyConditionExpression).toEqual(`begins_with(#bar, ${eqKey})`);

        // filter expression assertions
        const [eqfilterKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, eqLookup);
        const [beginsKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, beginsWithLookup);

        const [lowerRangeKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(capturedParam.ExpressionAttributeValues!, upperRange);

        expect(capturedParam.FilterExpression).toEqual(
            '((attribute_exists(#bop)) AND ' +
            `(#bar = ${eqfilterKey}) AND ` +
            `(#baz BETWEEN ${lowerRangeKey} AND ` +
            `${upperRangeKey})) ` +
            `OR (begins_with(#bop, ${beginsKey}))`);



    });
});

function captureParamAs<U>(spy: jest.SpyInstance, mockFn: jest.Mock): U {
    expect(spy).toHaveBeenCalledTimes(1);
    const mockFnCalls = mockFn.mock.calls;
    expect(mockFnCalls.length).toBeGreaterThan(0); //calls
    expect(mockFnCalls[0].length).toBeGreaterThan(0); //params of 1st call
    return mockFnCalls[0][0] as U;
}
