import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import {
  DynamoEntry,
  DynamoIndexes,
  DynamoKeysFrom,
  DynamoMapDefinition,
} from './type-mapping';
import { DynamoClientConfig, DynamoDefinition } from './dynamo-client-config';
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import DeleteItemInput = DocumentClient.DeleteItemInput;
import { ComparisonBuilder, conditionalParts } from './comparison';
import { AttributeBuilder } from './attribute-builder';
import { CompareWrapperOperator } from './operation';
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;

export type DeleteItemOptions<
  DEFINITION extends DynamoMapDefinition,
  RETURN_OLD extends boolean = false,
> = Pick<
  DeleteItemInput,
  | 'ExpressionAttributeNames'
  | 'ExpressionAttributeValues'
  | 'ReturnConsumedCapacity'
  | 'ReturnItemCollectionMetrics'
> & {
  condition?: (
    compare: () => ComparisonBuilder<DynamoEntry<DEFINITION>>,
  ) => CompareWrapperOperator<DynamoEntry<DEFINITION>>;
  returnOldValues?: RETURN_OLD;
};

export class DynamoDeleter {
  static async delete<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
    RETURN_OLD extends boolean = false,
  >(
    config: DynamoClientConfig<DEFINITION>,
    definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
    key: DynamoKeysFrom<DEFINITION, HASH, RANGE>,
    options: DeleteItemOptions<DEFINITION, RETURN_OLD>,
  ): Promise<
    {
      consumedCapacity?: ConsumedCapacity;
      itemCollectionMetrics?: ItemCollectionMetrics;
    } & (RETURN_OLD extends true
      ? { item: DynamoClientConfig<DEFINITION>['tableType'] }
      : {})
  > {
    const attributeBuilder = AttributeBuilder.create();
    const conditionPart =
      options.condition &&
      conditionalParts(definition, attributeBuilder, options.condition);
    const deleteInput: DeleteItemInput = {
      TableName: config.tableName,
      Key: key,
      ...attributeBuilder.asInput(),
      ...(conditionPart ? { ConditionExpression: conditionPart } : {}),
      ...(options.returnOldValues ? { ReturnValues: 'ALL_OLD' } : {}),
    };
    if (config.logStatements) {
      console.log(`DeleteItemInput: ${JSON.stringify(deleteInput, null, 2)}`);
    }
    const result = await config.client.delete(deleteInput).promise();
    return {
      ...(options.returnOldValues ? { item: result.Attributes } : {}),
      consumedCapacity: result.ConsumedCapacity,
      itemCollectionMetrics: result.ItemCollectionMetrics,
    } as any;
  }
}
