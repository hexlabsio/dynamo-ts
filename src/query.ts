import {KeyComparisonBuilder} from "./comparison";
import {QueryInput} from "aws-sdk/clients/dynamodb";
import {DynamoFilter} from "./filter";

type HashComparison<HASH extends keyof T, T> = {
    [K in HASH]: T[K]
};

type RangeComparison<R extends keyof T, T> = {
    [K in R]?: (sortKey: KeyComparisonBuilder<T[R]>) => any;
};

type RangeComparisonIfExists<R extends keyof T | null, T> =
    R extends string
        ? RangeComparison<R, T>
            : { }

type Filter<T, HASH extends keyof T, RANGE extends keyof T | null> = {
    filter?: DynamoFilter<T, HASH, RANGE>
}

type ExcessParameters = Omit<
    QueryInput,
    | 'TableName'
    | 'IndexName'
    | 'KeyConditionExpression'
    | 'ProjectionExpression'
    | 'FilterExpression'
    | 'ExclusiveStartKey'
    >;

export type QueryParametersInput<T, HASH extends keyof T, RANGE extends keyof T | null, PROJECTIONS extends (keyof T)[] | null> =
    HashComparison<HASH, T> &
    RangeComparisonIfExists<RANGE, T> &
    Filter<T,HASH, RANGE> &
    {
        projection?: PROJECTIONS;
        next?: string
        dynamo?: ExcessParameters
    }

export type QueryAllParametersInput<T, HASH extends keyof T, RANGE extends keyof T | null, PROJECTIONS extends (keyof T)[] | null> =
    QueryParametersInput<T, HASH, RANGE, PROJECTIONS> &
    { queryLimit? : number }
