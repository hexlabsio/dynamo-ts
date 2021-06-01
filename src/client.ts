import { DocumentClient } from "aws-sdk/clients/dynamodb";


import IndexName = DocumentClient.IndexName;
import Key = DocumentClient.Key;
import QueryInput = DocumentClient.QueryInput;
import AttributeMap = DocumentClient.AttributeMap;
import { FilterExpressions, KeyExpressions } from "./parsers";
import { Conditions, KeyConditions } from "./dynamoTypes";


export type QueryOptions<T> = {
    filters?: Conditions<T>,
    index?: IndexName,
    limit?: number,
    sort?: 'asc' | 'desc',
    projection?: Extract<keyof T, string>[],
    offsetKey?: Key;
};


export type QueryResult<T> = {
    items: T[],
    offsetKey?: Key;
};

export async function query<T>(
    this: DocumentClient,
    tableName: string,
    key: KeyConditions<T>,
    options: QueryOptions<T> = {},
    transform: (attributeMap: AttributeMap) => T = attributeMap => attributeMap as T): Promise<QueryResult<T>> {
    const queryInput = queryInputFrom(tableName, key, options);
    return await recursiveQuery(this, queryInput, transform);
}

async function recursiveQuery<T>(documentClient: DocumentClient, queryInput: QueryInput,
    transform: (attributeMap: AttributeMap) => T,
    results: T[] = []): Promise<QueryResult<T>> {
    console.log(`query input: ${JSON.stringify(queryInput, null, 2)}`);
    const queryOutput = await documentClient.query(queryInput).promise();
    const queryResults = queryOutput?.Items?.map(transform) ?? [];
    const queryResultLength = queryResults.length;
    console.log(`results found: ${queryResultLength}:  ${queryOutput?.Count}`)
    if (queryOutput.LastEvaluatedKey && queryInput.Limit && queryResultLength > 0 && queryResultLength < queryInput.Limit) {
        return await recursiveQuery(documentClient, {
            ...queryInput,
            Limit: queryInput.Limit - queryResultLength,
            ExclusiveStartKey: queryOutput.LastEvaluatedKey
        }, transform, [...results, ...queryResults]);
    } else {
        return {
            items: [...results, ...queryResults],
            offsetKey: queryOutput.LastEvaluatedKey
        };
    }
}

function queryInputFrom<T>(tableName: string, key: KeyConditions<T>, options: QueryOptions<T>): QueryInput {
    const keyParser = new KeyExpressions.Parser(key);
    const queryInput: QueryInput = {
        TableName: tableName,
        KeyConditionExpression: keyParser.expression,
        ExpressionAttributeValues: keyParser.expressionAttributeValues,
        ExpressionAttributeNames: keyParser.expressionAttributeNames,
        IndexName: options.index,
        ExclusiveStartKey: options.offsetKey,
        ProjectionExpression: options?.projection?.join(", ")
    };

    if (options.filters) {
        const filterParser = new FilterExpressions.Parser(options.filters);
        Object.assign(queryInput.ExpressionAttributeNames, filterParser.expressionAttributeNames);
        Object.assign(queryInput.ExpressionAttributeValues, filterParser.expressionAttributeValues);
        queryInput.FilterExpression = filterParser.expression;
    }
    if (options.sort) {
        queryInput.ScanIndexForward = options.sort === 'asc';
    }

    if (options.limit && options.limit > 0) {
        queryInput.Limit = options.limit;
    }
    return queryInput;
}

declare module 'aws-sdk/clients/dynamodb' {
    interface DocumentClient {
        typedQuery: typeof query;
    }
}

DocumentClient.prototype.typedQuery = query;