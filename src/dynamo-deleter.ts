import { DeleteCommandInput, DeleteCommandOutput } from "@aws-sdk/lib-dynamodb";
import { AttributeBuilder } from './attribute-builder';
import { filterPartsWithKey } from "./comparison";
import { DynamoFilter2 } from './filter';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoInfo,
  PickKeys,
  TypeFromDefinition,
} from './types';

export type DeleteReturnValues = 'NONE' | 'ALL_OLD';

export type DeleteItemOptions<
  INFO extends DynamoInfo,
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
  condition?: DynamoFilter2<INFO>;
};

export type DeleteItemReturn<
  INFO extends DynamoInfo,
  RETURN extends DeleteReturnValues,
> = CamelCaseKeys<
  Pick<DeleteCommandOutput, 'ConsumedCapacity' | 'ItemCollectionMetrics'>
> &
  (RETURN extends 'ALL_OLD'
    ? { item?: TypeFromDefinition<INFO['definition']> }
    : {});

export interface DeleteExecutor<
  T extends DynamoInfo,
  RETURN extends DeleteReturnValues,
> {
  input: DeleteCommandInput;
  execute(): Promise<DeleteItemReturn<T, RETURN>>;
}

export class DynamoDeleter<T extends DynamoInfo> {
  constructor(
    private readonly info: T,
    private readonly config: DynamoConfig,
  ) {}

  async delete<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: PickKeys<T>,
    options: DeleteItemOptions<T, RETURN> = {},
  ): Promise<DeleteItemReturn<T, RETURN>> {
    const getInput = this.deleteExecutor(keys, options);
    if (this.config.logStatements) {
      console.log(
        `DeleteItemInput: ${JSON.stringify(getInput.input, null, 2)}`,
      );
    }
    return await getInput.execute();
  }

  deleteExecutor<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: PickKeys<T>,
    options: DeleteItemOptions<T, RETURN> = {},
  ): DeleteExecutor<T, RETURN> {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition &&
      filterPartsWithKey(this.info, attributeBuilder, options.condition);
    const input: DeleteCommandInput = {
      ...attributeBuilder.asInput(),
      TableName: this.config.tableName,
      Key: keys,
      ReturnValues: options.returnValues,
      ConditionExpression: condition,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<DeleteItemReturn<T, RETURN>> {
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
