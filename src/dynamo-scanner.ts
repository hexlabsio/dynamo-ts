import {
  DynamoEntry,
  DynamoIndexes,
  DynamoMapDefinition,
} from './type-mapping';
import { DynamoClientConfig, DynamoDefinition } from './dynamo-client-config';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import { ComparisonBuilder, conditionalParts } from './comparison';
import { AttributeBuilder } from './attribute-builder';
import { CompareWrapperOperator } from './operation';
import QueryInput = DocumentClient.QueryInput;
import ScanInput = DocumentClient.ScanInput;
import { Projection, ProjectionHandler } from './projector';

export type ScanOptions<
  DEFINITION extends DynamoMapDefinition,
  PROJECTED,
> = Omit<QueryInput, 'TableName'> & {
  projection?: Projection<DEFINITION, PROJECTED>;
  filter?: (
    compare: () => ComparisonBuilder<DynamoEntry<DEFINITION>>,
  ) => CompareWrapperOperator<DynamoEntry<DEFINITION>>;
  next?: string;
};

export class DynamoScanner {
  static async scan<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
    RETURN_OLD extends boolean = false,
    PROJECTED = null,
  >(
    config: DynamoClientConfig<DEFINITION>,
    definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
    options: ScanOptions<DEFINITION, PROJECTED> = {},
  ): Promise<{
    next?: string;
    member: {
      [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K];
    }[];
  }> {
    const attributeBuilder = AttributeBuilder.create();
    const projection = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      config.definition,
      options.projection,
    );
    const {
      filter,
      next,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ...extras
    } = options;
    const conditionPart =
      filter && conditionalParts(definition, attributeBuilder, filter);
    const scanInput: ScanInput = {
      TableName: config.tableName,
      ...(conditionPart ? { FilterExpression: conditionPart } : {}),
      ProjectionExpression: projection,
      ...(next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(next, 'base64').toString('ascii'),
            ),
          }
        : {}),
      ...extras,
      ...attributeBuilder.asInput({
        ExpressionAttributeNames,
        ExpressionAttributeValues,
      }),
    };
    if (config.logStatements) {
      console.log(`ScanInput: ${JSON.stringify(scanInput, null, 2)}`);
    }
    const result = await config.client.scan(scanInput).promise();
    return {
      member: (result.Items ?? []) as any,
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
            'base64',
          )
        : undefined,
    };
  }
}
