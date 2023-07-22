import { PutCommandInput, PutCommandOutput } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { DynamoFilter } from './filter';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoInfo,
  TypeFromDefinition,
} from './types';

export type PutReturnValues = 'NONE' | 'ALL_OLD';

export type PutItemOptions<
  INFO extends DynamoInfo,
  RETURN extends PutReturnValues,
> = CamelCaseKeys<
  Pick<
    PutCommandInput,
    'ReturnItemCollectionMetrics' | 'ReturnConsumedCapacity'
  >
> & {
  returnValues?: RETURN;
  condition?: DynamoFilter<INFO>;
};

export type PutItemReturn<
  INFO extends DynamoInfo,
  RETURN extends PutReturnValues,
> = CamelCaseKeys<
  Pick<PutCommandOutput, 'ConsumedCapacity' | 'ItemCollectionMetrics'>
> &
  (RETURN extends 'ALL_OLD'
    ? { item: TypeFromDefinition<INFO['definition']> | undefined }
    : {});

export interface PutExecutor<
  T extends DynamoInfo,
  RETURN extends PutReturnValues,
> {
  input: PutCommandInput;
  execute(): Promise<PutItemReturn<T, RETURN>>;
}

export class DynamoPuter<T extends DynamoInfo> {
  constructor(
    private readonly info: T,
    private readonly config: DynamoConfig,
  ) {}

  async put<RETURN extends PutReturnValues = 'NONE'>(
    item: TypeFromDefinition<T['definition']>,
    options: PutItemOptions<T, RETURN> = {},
  ): Promise<PutItemReturn<T, RETURN>> {
    const getInput = this.putExecutor(item, options);
    if (this.config.logStatements) {
      console.log(`PutItemInput: ${JSON.stringify(getInput.input, null, 2)}`);
    }
    return await getInput.execute();
  }

  putExecutor<RETURN extends PutReturnValues = 'NONE'>(
    item: TypeFromDefinition<T['definition']>,
    options: PutItemOptions<T, RETURN> = {},
  ): PutExecutor<T, RETURN> {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition &&
      filterParts(this.info, attributeBuilder, options.condition);
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
      async execute(): Promise<PutItemReturn<T, RETURN>> {
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
