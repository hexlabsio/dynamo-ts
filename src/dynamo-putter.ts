import {
  DynamoEntry, DynamoIndexes,
  DynamoMapDefinition,
} from "./type-mapping";
import {DynamoClientConfig, DynamoDefinition} from "./dynamo-client-config";
import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import PutItemInput = DocumentClient.PutItemInput;
import {ComparisonBuilder, conditionalParts} from "./comparison";
import {AttributeBuilder} from "./naming";
import {CompareWrapperOperator} from "./operation";

export type PutItemExtras<
    DEFINITION  extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    RETURN_OLD extends boolean = false
  > =
    Pick<PutItemInput, 'ExpressionAttributeNames' | 'ExpressionAttributeValues'> &
    {
      condition?: (compare: () => ComparisonBuilder<DEFINITION>) => CompareWrapperOperator<DEFINITION>,
      returnOldValues?: RETURN_OLD
    }

export class DynamoPutter {

  static async put
  <
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null,
      INDEXES extends DynamoIndexes<DEFINITION> = null,
      RETURN_OLD extends boolean = false
  > (
      config: DynamoClientConfig<DEFINITION>,
      definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
      attributeBuilder: AttributeBuilder,
      item: DynamoEntry<DEFINITION>,
      options: PutItemExtras<DEFINITION, HASH, RANGE, RETURN_OLD> = {}
  ) : Promise<RETURN_OLD extends true ? { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] } : void> {
    const {ExpressionAttributeNames, ExpressionAttributeValues} = options;
    const conditionPart = options.condition && conditionalParts(definition, attributeBuilder, options.condition);
    const putInput: PutItemInput = {
      TableName: config.tableName,
      Item: item,
      ...(conditionPart?.attributeBuilder.asInput({ExpressionAttributeNames, ExpressionAttributeValues}) ?? {}),
      ...(conditionPart ? {ConditionExpression: conditionPart.expression} : {}),
      ...(options.returnOldValues ? {ReturnValues: 'ALL_OLD'} : {})
    };
    if(config.logStatements) {
      console.log(`PutItemInput: ${JSON.stringify(putInput, null, 2)}`)
    }
    const result = await config.client
        .put(putInput)
        .promise();
    if(options.returnOldValues)
      return result.Attributes as any;
    return undefined as any;
  }

}