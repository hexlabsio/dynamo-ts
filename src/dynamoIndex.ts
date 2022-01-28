import {ComparisonBuilder, KeyComparisonBuilder} from "./comparison";
import {CompareWrapperOperator} from "./operation";
import {QueryInput} from "aws-sdk/clients/dynamodb";
import {QueryAllParametersInput} from "./query";

export interface DynamoTableIndex<
    T,
    H extends keyof T,
    R extends keyof T | null = null,
    > {
    query<P extends (keyof T)[] | null = null>(
        queryParameters: { [K in H]: T[K] } &
            (R extends string
                ? { [K in R]?: (sortKey: KeyComparisonBuilder<T[R]>) => any }
                : {}) & {
            filter?: (
                compare: () => ComparisonBuilder<
                    Omit<T, R extends string ? H | R : H>
                    >,
            ) => CompareWrapperOperator<Omit<T, R extends string ? H | R : H>>;
        } & { projection?: P; next?: string } & {
            dynamo?: Omit<
                QueryInput,
                | 'TableName'
                | 'IndexName'
                | 'KeyConditionExpression'
                | 'ProjectionExpression'
                | 'FilterExpression'
                | 'ExclusiveStartKey'
                >;
        },
    ): Promise<{
        next?: string;
        member: P extends (keyof T)[]
            ? { [K in R extends string ? P[number] | H | R : P[number] | H]: T[K] }[]
            : { [K in keyof T]: T[K] }[];
    }>;

    queryAll<P extends (keyof T)[] | null = null>(
        queryParameters: QueryAllParametersInput<T, H, R, P>
    ): Promise<{
        next?: string;
        member: P extends (keyof T)[]
            ? { [K in P[number]]: T[K] }[]
            : { [K in keyof T]: T[K] }[];
    }>;

}