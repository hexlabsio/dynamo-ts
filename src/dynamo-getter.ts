import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import GetItemInput = DocumentClient.GetItemInput;
import {
  DynamoEntry,
  DynamoKeysFrom,
  DynamoMapDefinition,
  DynamoRangeKey
} from "./type-mapping";
import {DynamoClientConfig} from "./dynamo-client-config";
import {AttributeBuilder} from "./attribute-builder";
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import BatchGetItemInput = DocumentClient.BatchGetItemInput;
import {Projection, ProjectionHandler} from "./projector";

export type GetItemExtras<DEFINITION extends DynamoMapDefinition, PROJECTED> = Pick<GetItemInput, 'ConsistentRead' | 'ReturnConsumedCapacity' | 'ExpressionAttributeNames'> & {
  projection?: Projection<DEFINITION, PROJECTED>
};

export class DynamoGetter {

  static async get
  <
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends DynamoRangeKey<DEFINITION,HASH>,
      PROJECTED = null
  > (
      config: DynamoClientConfig<DEFINITION>,
      key: DynamoKeysFrom<DEFINITION, HASH, RANGE>,
      options: GetItemExtras<DEFINITION, PROJECTED> = {}
  ) : Promise<{item: (PROJECTED extends null ? DynamoClientConfig<DEFINITION>['tableType'] : PROJECTED) | undefined, consumedCapacity?: ConsumedCapacity}> {
    const attributeBuilder = AttributeBuilder.create();
    const expression = ProjectionHandler.projectionFor(attributeBuilder, config.definition, options.projection);
    const getInput: GetItemInput = {
      TableName: config.tableName,
      Key: key,
      ...options,
      ProjectionExpression: expression,
      ...attributeBuilder.asInput()
    };
    if(config.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(getInput, null, 2)}`)
    }
    const result = await config.client
        .get(getInput)
        .promise();
    return {item: result.Item as any, consumedCapacity: result.ConsumedCapacity};
  }

  async batchGet<
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends DynamoRangeKey<DEFINITION,HASH>
    >(
        config: DynamoClientConfig<DEFINITION>,
        keys: DynamoKeysFrom<DEFINITION, HASH, RANGE>[],
        returnConsumedCapacity?: BatchGetItemInput['ReturnConsumedCapacity'],
        consistent?: boolean
  ): Promise<{items: DynamoClientConfig<DEFINITION>['tableType'][], consumedCapacity?: ConsumedCapacity}>{
    const attributeBuilder = AttributeBuilder.create();
    const [, projection] = ProjectionHandler.projectionFor(attributeBuilder, config.definition);
    const batchGetInput: BatchGetItemInput = {
      ReturnConsumedCapacity: returnConsumedCapacity,
      RequestItems: { [config.tableName]: {
        ...attributeBuilder.asInput(),
        Keys: keys,
          ...(projection ? {ProjectionExpression: projection} : {}),
          ...((consistent !== undefined) ? {ConsistentRead: consistent}: {})
      }}
    }
    if(config.logStatements) {
      console.log(`BatchGetItemInput: ${JSON.stringify(batchGetInput, null, 2)}`)
    }
    const result = await config.client.batchGet(batchGetInput).promise();
    return result.Responses![config.tableName] as any;
  }

}