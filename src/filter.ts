import { ComparisonBuilder } from './comparison';
import { CompareWrapperOperator } from './operation';
import { DynamoEntry, DynamoMapDefinition } from './type-mapping';

type WithoutKeys<T, HASH extends keyof T, RANGE extends keyof T | null> = Omit<
  T,
  RANGE extends string ? HASH | RANGE : HASH
>;

export type DynamoFilter<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
> = (
  compare: () => ComparisonBuilder<WithoutKeys<DEFINITION, HASH, RANGE>>,
) => CompareWrapperOperator<WithoutKeys<DEFINITION, HASH, RANGE>>;
