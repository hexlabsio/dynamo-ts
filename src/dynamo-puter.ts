import { PutCommandInput, PutCommandOutput } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { DynamoFilter } from './types/filter';
import { TableDefinition } from './table-builder/table-definition';
import { DynamoConfig } from './types';
import { CamelCaseKeys } from './types/camel-case';


export type PutReturnValues = 'NONE' | 'ALL_OLD';

export type PutItemOptions<
  TableType,
  RETURN extends PutReturnValues,
> = CamelCaseKeys<
  Pick<
    PutCommandInput,
    'ReturnItemCollectionMetrics' | 'ReturnConsumedCapacity'
  >
> & {
  returnValues?: RETURN;
  condition?: DynamoFilter<TableType>;
};

export type PutItemReturn<
  TableType,
  RETURN extends PutReturnValues,
> = CamelCaseKeys<
  Pick<PutCommandOutput, 'ConsumedCapacity' | 'ItemCollectionMetrics'>
> &
  (RETURN extends 'ALL_OLD'
    ? { item: TableType | undefined }
    : {});

export interface PutExecutor<
  TableType,
  RETURN extends PutReturnValues,
> {
  input: PutCommandInput;
  execute(): Promise<PutItemReturn<TableType, RETURN>>;
}

export class DynamoPuter<TableConfig extends TableDefinition> {
  constructor(
    private readonly config: DynamoConfig,
  ) {}

  async put<RETURN extends PutReturnValues = 'NONE'>(
    item: TableConfig['type'],
    options: PutItemOptions<TableConfig['type'], RETURN> = {},
  ): Promise<PutItemReturn<TableConfig['type'], RETURN>> {
    const getInput = this.putExecutor(item, options);
    if (this.config.logStatements) {
      console.log(`PutItemInput: ${JSON.stringify(getInput.input, null, 2)}`);
    }
    return await getInput.execute();
  }

  putExecutor<RETURN extends PutReturnValues = 'NONE'>(
    item: TableConfig['type'],
    options: PutItemOptions<TableConfig['type'], RETURN> = {},
  ): PutExecutor<TableConfig['type'], RETURN> {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition &&
      filterParts(attributeBuilder, options.condition);
    const input: PutCommandInput = {
      ...attributeBuilder.asInput(),
      TableName: this.config.tableName,
      Item: item,
      ReturnValues: options.returnValues,
      ConditionExpression: condition,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<PutItemReturn<TableConfig['type'], RETURN>> {
        const result = await client.put(input);
        return {
          item: result.Attributes as any,
          consumedCapacity: result.ConsumedCapacity,
          itemCollectionMetrics: result.ItemCollectionMetrics,
        };
      },
    };
  }
}
