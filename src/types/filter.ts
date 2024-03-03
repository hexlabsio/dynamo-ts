import { ComparisonBuilderFrom } from '../comparison.js';
import { CompareWrapperOperator } from '../operation.js';

export type DynamoFilter<TableType> = (
  compare: () => ComparisonBuilderFrom<TableType>,
) => CompareWrapperOperator<TableType>;
