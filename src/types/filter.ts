import { ComparisonBuilderFrom } from '../comparison';
import { CompareWrapperOperator } from '../operation';

export type DynamoFilter<TableType> = (
  compare: () => ComparisonBuilderFrom<TableType>,
) => CompareWrapperOperator<TableType>;
