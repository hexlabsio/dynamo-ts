import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import GetItemInput = DocumentClient.GetItemInput;
import { AttributeBuilder } from './attribute-builder';
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import { Projection, ProjectionHandler } from './projector';
import { CamelCaseKeys, DynamoConfig, DynamoInfo, PickKeys, TypeFromDefinition } from './types';

export type GetItemOptions<INFO extends DynamoInfo, PROJECTION> = CamelCaseKeys<Pick<GetItemInput, 'ConsistentRead' | 'ReturnConsumedCapacity'>> & {
  projection?: Projection<INFO, PROJECTION>;
}

export type GetItemReturn<INFO extends DynamoInfo, PROJECTION> = {
  item: PROJECTION extends null ? TypeFromDefinition<INFO> : PROJECTION;
  consumedCapacity?: ConsumedCapacity;
}

export class DynamoGetter<T extends DynamoInfo> {
  constructor(private readonly info: T, private readonly config: DynamoConfig) {}
  async get<PROJECTION = null>(keys: PickKeys<T>, options: GetItemOptions<T, PROJECTION> = {}): Promise<GetItemReturn<T, PROJECTION>> {
    const getInput = this.getInput(keys, options);
    if (this.config.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(getInput, null, 2)}`);
    }
    const result = await this.config.client.get(getInput).promise();
    return {
      item: result.Item as any,
      consumedCapacity: result.ConsumedCapacity,
    };
  }

  getInput<PROJECTION = null>(keys: PickKeys<T>, options: GetItemOptions<T, PROJECTION>): GetItemInput {
    const attributeBuilder = AttributeBuilder.create();
    const expression = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    return {
      TableName: this.config.tableName,
      Key: keys,
      ...options,
      ProjectionExpression: expression,
      ...attributeBuilder.asInput(),
    };
  }
}