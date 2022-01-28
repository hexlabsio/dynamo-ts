import {ComparisonBuilder} from "./comparison";
import {CompareWrapperOperator} from "./operation";

type WithoutKeys<T, HASH extends keyof T, RANGE extends keyof T | null> = Omit<T, RANGE extends string ? HASH | RANGE : HASH>

export type DynamoFilter<T, HASH extends keyof T, RANGE extends keyof T | null> = (
    compare: () => ComparisonBuilder<WithoutKeys<T,HASH, RANGE>>,
) => CompareWrapperOperator<WithoutKeys<T,HASH, RANGE>>;