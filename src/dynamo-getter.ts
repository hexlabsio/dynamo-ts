import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import GetItemInput = DocumentClient.GetItemInput;
import {
  DynamoEntry,
  DynamoKeysFrom,
  DynamoMapDefinition,
  DynamoRangeKey
} from "./type-mapping";
import {DynamoClientConfig} from "./dynamo-client-config";
import {AttributeBuilder} from "./naming";
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import BatchGetItemInput = DocumentClient.BatchGetItemInput;

export type GetItemExtras = Pick<GetItemInput, 'ConsistentRead' | 'ReturnConsumedCapacity' | 'ProjectionExpression' | 'ExpressionAttributeNames'>;

export class ProjectionHandler {
  static projectionFor<DEFINITION>(attributeBuilder: AttributeBuilder, definition: DEFINITION, expression?: string): [AttributeBuilder, string]{
    if(expression) {
      //complex projection, take users input
      return [attributeBuilder, expression];
    }
    const keys = Object.keys(definition);
    const updatedAttributes = attributeBuilder.addNames(...keys);
    return [updatedAttributes, keys.map(key => updatedAttributes.nameFor(key)).join(',')];
  }
}

export class DynamoGetter {

  static async get
  <
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends DynamoRangeKey<DEFINITION,HASH>
  > (
      config: DynamoClientConfig<DEFINITION>,
      key: DynamoKeysFrom<DEFINITION, HASH, RANGE>,
      options: GetItemExtras = {}
  ) : Promise<{item: DynamoClientConfig<DEFINITION>['tableType'] | undefined, consumedCapacity?: ConsumedCapacity}> {
    const [attributes, projection] = ProjectionHandler.projectionFor(AttributeBuilder.create(), config.definition, options.ProjectionExpression);
    const getInput: GetItemInput = {
      TableName: config.tableName,
      Key: key,
      ...options,
      ProjectionExpression: projection,
      ...attributes.asInput()
    };
    if(config.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(getInput, null, 2)}`)
    }
    const result = await config.client
        .get(getInput)
        .promise();
    return {item: result.Item as DynamoClientConfig<DEFINITION>['tableType'] | undefined, consumedCapacity: result.ConsumedCapacity};
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

    const [attributes, projection] = ProjectionHandler.projectionFor(AttributeBuilder.create(), config.definition);
    const batchGetInput: BatchGetItemInput = {
      ReturnConsumedCapacity: returnConsumedCapacity,
      RequestItems: { [config.tableName]: {
        ...attributes.asInput(),
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