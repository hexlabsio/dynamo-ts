import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { DynamoFilter2 } from './filter';
import { CamelCaseKeys, DynamoConfig, DynamoInfo, PickKeys, TypeFromDefinition } from './types';
import DeleteItemInput = DocumentClient.DeleteItemInput;
import DeleteItemOutput = DocumentClient.DeleteItemOutput;

export type DeleteReturnValues = 'NONE' | 'ALL_OLD';

export type DeleteItemOptions<INFO extends DynamoInfo, RETURN extends DeleteReturnValues> = Partial<CamelCaseKeys<Pick<DeleteItemInput, 'ReturnItemCollectionMetrics' | 'ReturnConsumedCapacity'>>> & {
  returnValues?: RETURN;
  condition?: DynamoFilter2<INFO>
}

export type DeleteItemReturn<INFO extends DynamoInfo, RETURN extends DeleteReturnValues> =
  CamelCaseKeys<Pick<DeleteItemOutput, 'ConsumedCapacity' | 'ItemCollectionMetrics'>>
  & (RETURN extends 'ALL_OLD' ? { item?: TypeFromDefinition<INFO['definition']> } : {})

export interface DeleteExecutor<T extends DynamoInfo, RETURN extends DeleteReturnValues> {
  input: DeleteItemInput;
  execute(): Promise<DeleteItemReturn<T, RETURN>>
}

export class DynamoDeleter<T extends DynamoInfo> {
  constructor(private readonly info: T, private readonly config: DynamoConfig) {}

  async delete<RETURN extends DeleteReturnValues = "NONE">(keys: PickKeys<T>, options: DeleteItemOptions<T, RETURN> = {}): Promise<DeleteItemReturn<T, RETURN>> {
    const getInput = this.deleteExecutor(keys, options);
    if (this.config.logStatements) {
      console.log(`DeleteItemInput: ${JSON.stringify(getInput.input, null, 2)}`);
    }
    return await getInput.execute();
  }

  deleteExecutor<RETURN extends DeleteReturnValues = "NONE">(keys: PickKeys<T>, options: DeleteItemOptions<T, RETURN> = {}): DeleteExecutor<T, RETURN> {
    const attributeBuilder = AttributeBuilder.create();
    const condition = options.condition && filterParts(this.info, attributeBuilder, options.condition);
    const input: DeleteItemInput = {
      ...attributeBuilder.asInput(),
      TableName: this.config.tableName,
      Key: keys,
      ReturnValues: options.returnValues,
      ConditionExpression: condition,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<DeleteItemReturn<T, RETURN>> {
        const result = await client.delete(input).promise();
        return {
          item: result.Attributes as any,
          consumedCapacity: result.ConsumedCapacity,
          itemCollectionMetrics: result.ItemCollectionMetrics
        };
      }
    }
  }
}