import {
  DynamoEntry,
  DynamoIndexes,
  DynamoMapDefinition,
} from './type-mapping';
import { DynamoClientConfig, DynamoDefinition } from './dynamo-client-config';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import PutItemInput = DocumentClient.PutItemInput;
import { ComparisonBuilder, conditionalParts } from './comparison';
import { AttributeBuilder } from './attribute-builder';
import { CompareWrapperOperator } from './operation';
import ReturnConsumedCapacity = DocumentClient.ReturnConsumedCapacity;
import ReturnItemCollectionMetrics = DocumentClient.ReturnItemCollectionMetrics;
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;

export type PutItemExtras<
  DEFINITION extends DynamoMapDefinition,
  RETURN_OLD extends boolean = false,
> = {
  condition?: (
    compare: () => ComparisonBuilder<DynamoEntry<DEFINITION>>,
  ) => CompareWrapperOperator<DynamoEntry<DEFINITION>>;
  returnOldValues?: RETURN_OLD;
  returnConsumedCapacity?: ReturnConsumedCapacity;
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics;
};

export type PutItemResult<
  DEFINITION extends DynamoMapDefinition,
  RETURN_OLD extends boolean = false,
> = {
  item: RETURN_OLD extends true
    ? { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }
    : undefined;
  consumedCapacity?: ConsumedCapacity;
  itemCollectionMetrics?: ItemCollectionMetrics;
};

export class DynamoPutter {
  static async put<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
    RETURN_OLD extends boolean = false,
  >(
    config: DynamoClientConfig<DEFINITION>,
    definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
    item: DynamoEntry<DEFINITION>,
    options: PutItemExtras<DEFINITION, RETURN_OLD> = {},
  ): Promise<PutItemResult<DEFINITION, RETURN_OLD>> {
    const attributeBuilder = AttributeBuilder.create();
    const conditionPart =
      options.condition &&
      conditionalParts(definition, attributeBuilder, options.condition);
    const putInput: PutItemInput = {
      TableName: config.tableName,
      Item: item,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
      ...attributeBuilder.asInput(),
      ...(conditionPart ? { ConditionExpression: conditionPart } : {}),
      ...(options.returnOldValues ? { ReturnValues: 'ALL_OLD' } : {}),
    };
    if (config.logStatements) {
      console.log(`PutItemInput: ${JSON.stringify(putInput, null, 2)}`);
    }
    const result = await config.client.put(putInput).promise();
    return {
      item: options.returnOldValues ? (result.Attributes as any) : undefined,
      consumedCapacity: result.ConsumedCapacity,
      itemCollectionMetrics: result.ItemCollectionMetrics,
    };
  }
}
