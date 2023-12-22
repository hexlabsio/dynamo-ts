import { DeleteCommandInput, DeleteCommandOutput } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { TableDefinition } from './table-builder/table-definition';
import { CamelCaseKeys } from './types/camel-case';
import { DynamoConfig, DynamoFilter } from './types';

export type DeleteReturnValues = 'NONE' | 'ALL_OLD';

export type DeleteItemOptions<
  TableType,
  RETURN extends DeleteReturnValues,
> = Partial<
  CamelCaseKeys<
    Pick<
      DeleteCommandInput,
      'ReturnItemCollectionMetrics' | 'ReturnConsumedCapacity'
    >
  >
> & {
  returnValues?: RETURN;
  condition?: DynamoFilter<TableType>;
};

export type DeleteItemReturn<
  TableType,
  RETURN extends DeleteReturnValues,
> = CamelCaseKeys<
  Pick<DeleteCommandOutput, 'ConsumedCapacity' | 'ItemCollectionMetrics'>
> &
  (RETURN extends 'ALL_OLD' ? { item?: TableType } : {});

export interface DeleteExecutor<TableType, RETURN extends DeleteReturnValues> {
  input: DeleteCommandInput;
  execute(): Promise<DeleteItemReturn<TableType, RETURN>>;
}

export class DynamoDeleter<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  async delete<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: TableConfig['keys'],
    options: DeleteItemOptions<TableConfig['type'], RETURN> = {},
  ): Promise<DeleteItemReturn<TableConfig['type'], RETURN>> {
    const getInput = this.deleteExecutor(keys, options);
    if (this.clientConfig.logStatements) {
      console.log(
        `DeleteItemInput: ${JSON.stringify(getInput.input, null, 2)}`,
      );
    }
    return await getInput.execute();
  }

  deleteExecutor<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: TableConfig['keys'],
    options: DeleteItemOptions<TableConfig['type'], RETURN> = {},
  ): DeleteExecutor<TableConfig['type'], RETURN> {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition && filterParts(attributeBuilder, options.condition);
    const input: DeleteCommandInput = {
      ...attributeBuilder.asInput(),
      TableName: this.clientConfig.tableName,
      Key: keys,
      ReturnValues: options.returnValues,
      ConditionExpression: condition,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.clientConfig.client;
    return {
      input,
      async execute(): Promise<DeleteItemReturn<TableConfig['type'], RETURN>> {
        const result = await client.delete(input);
        return {
          item: result.Attributes as any,
          consumedCapacity: result.ConsumedCapacity,
          itemCollectionMetrics: result.ItemCollectionMetrics,
        };
      },
    };
  }
}
