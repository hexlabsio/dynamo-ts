import { ComparisonBuilder, ComparisonBuilderFrom } from './comparison';
import { CompareWrapperOperator } from './operation';
import { DynamoEntry, DynamoMapDefinition } from './type-mapping';
import { DynamoInfo, TypeFromDefinition } from './types';

type WithoutKeys<T, HASH extends keyof T, RANGE extends keyof T | null> = Omit<
  T,
  RANGE extends string ? HASH | RANGE : HASH
>;

export type DynamoFilter<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
> = (
  compare: () => ComparisonBuilder<
    WithoutKeys<DynamoEntry<DEFINITION>, HASH, RANGE>
  >,
) => CompareWrapperOperator<WithoutKeys<DynamoEntry<DEFINITION>, HASH, RANGE>>;

export type DynamoFilter2<D extends DynamoInfo> = (
  compare: () => ComparisonBuilderFrom<D>
) => CompareWrapperOperator<TypeFromDefinition<D>>;