import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import {
  DynamoEntry,
  DynamoKeysFrom,
  DynamoMapDefinition
} from "./type-mapping";
import {DynamoClientConfig} from "./dynamo-client-config";
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import DeleteItemInput = DocumentClient.DeleteItemInput;
import {ComparisonBuilder} from "./comparison";
import {CompareWrapperOperator} from "./operation";
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;

export type DeleteItemOptions<DEFINITION extends DynamoMapDefinition, RETURN_OLD extends boolean = false> = Pick<DeleteItemInput, 'ExpressionAttributeNames' | 'ExpressionAttributeValues' | 'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'> &
    {
      condition?: (compare: () => ComparisonBuilder<DEFINITION>) => CompareWrapperOperator<DEFINITION>,
      returnOldValues?: RETURN_OLD
    }

export class DynamoDeleter {

  static async delete<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
  RETURN_OLD extends boolean = false
  >(
      config: DynamoClientConfig<DEFINITION>,
      key: DynamoKeysFrom<DEFINITION, HASH, RANGE>,
      options: DeleteItemOptions<DEFINITION, RETURN_OLD>
  ) : Promise<{consumedCapacity?: ConsumedCapacity, itemCollectionMetrics?: ItemCollectionMetrics} & (RETURN_OLD extends true ? {item: DynamoClientConfig<DEFINITION>['tableType']} : {})> {
    const deleteInput: DeleteItemInput = { TableName: config.tableName, Key: key, ...options};
    if(config.logStatements) {
      console.log(`DeleteItemInput: ${JSON.stringify(deleteInput, null, 2)}`);
    }
    const result = await config.client.delete(deleteInput).promise();
    return {
      ...(options.returnOldValues ? {item: result.Attributes} : {}),
      consumedCapacity: result.ConsumedCapacity,
      itemCollectionMetrics: result.ItemCollectionMetrics
    } as any;
  }

}