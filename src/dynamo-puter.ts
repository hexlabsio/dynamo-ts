import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { DynamoFilter2 } from './filter';
import { CamelCaseKeys, DynamoConfig, DynamoInfo, TypeFromDefinition } from './types';
import PutItemInput = DocumentClient.PutItemInput;
import PutItemOutput = DocumentClient.PutItemOutput;

export type PutReturnValues = 'NONE' | 'ALL_OLD';

export type PutItemOptions<INFO extends DynamoInfo, RETURN extends PutReturnValues> = CamelCaseKeys<Pick<PutItemInput, 'ReturnItemCollectionMetrics' | 'ReturnConsumedCapacity'>> & {
  returnValues?: RETURN;
  condition?: DynamoFilter2<INFO>
}

export type PutItemReturn<INFO extends DynamoInfo, RETURN extends PutReturnValues> =
  CamelCaseKeys<Pick<PutItemOutput, 'ConsumedCapacity' | 'ItemCollectionMetrics'>>
  & (RETURN extends 'ALL_OLD' ? { item: TypeFromDefinition<INFO['definition']> | undefined } : {})

export interface PutExecutor<T extends DynamoInfo, RETURN extends PutReturnValues> {
  input: PutItemInput;
  execute(): Promise<PutItemReturn<T, RETURN>>
}

export class DynamoPuter<T extends DynamoInfo> {
  constructor(private readonly info: T, private readonly config: DynamoConfig) {}

  async put<RETURN extends PutReturnValues = "NONE">(item: TypeFromDefinition<T['definition']>, options: PutItemOptions<T, RETURN> = {}): Promise<PutItemReturn<T, RETURN>> {
    const getInput = this.putExecutor(item, options);
    if (this.config.logStatements) {
      console.log(`PutItemInput: ${JSON.stringify(getInput.input, null, 2)}`);
    }
    return await getInput.execute();
  }

  putExecutor<RETURN extends PutReturnValues = "NONE">(item: TypeFromDefinition<T['definition']>, options: PutItemOptions<T, RETURN> = {}): PutExecutor<T, RETURN> {
    const attributeBuilder = AttributeBuilder.create();
    const condition = options.condition && filterParts(this.info, attributeBuilder, options.condition);
    const input: PutItemInput = {
      ...attributeBuilder.asInput(),
      TableName: this.config.tableName,
      Item: item,
      ReturnValues: options.returnValues,
      ConditionExpression: condition,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<PutItemReturn<T, RETURN>> {
        const result = await client.put(input).promise();
        return {
          item: result.Attributes as any,
          consumedCapacity: result.ConsumedCapacity,
          itemCollectionMetrics: result.ItemCollectionMetrics
        };
      }
    }
  }
}