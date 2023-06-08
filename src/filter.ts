import { ComparisonBuilderFrom } from './comparison';
import { CompareWrapperOperator } from './operation';
import { DynamoInfo, TypeFromDefinition } from './types';

export type DynamoFilter<D extends DynamoInfo> = (
  compare: () => ComparisonBuilderFrom<D>,
) => CompareWrapperOperator<TypeFromDefinition<D>>;
