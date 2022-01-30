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

export type GetItemExtras = Omit<GetItemInput, 'TableName' | 'Key'>;

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
  ) : Promise<DynamoClientConfig<DEFINITION>['tableType'] | undefined> {
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
    return result.Item as DynamoClientConfig<DEFINITION>['tableType'] | undefined;
  }

}