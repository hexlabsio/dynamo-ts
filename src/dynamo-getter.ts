import { DynamoDB } from 'aws-sdk';
import GetItemInput = DynamoDB.DocumentClient.GetItemInput;
import ConsumedCapacity = DynamoDB.DocumentClient.ConsumedCapacity;
import { AttributeBuilder } from './attribute-builder';
import { Projection, ProjectionHandler } from './projector';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoInfo,
  PickKeys,
  TypeFromDefinition,
} from './types';

export type GetItemOptions<INFO extends DynamoInfo, PROJECTION> = Partial<
  CamelCaseKeys<Pick<GetItemInput, 'ConsistentRead' | 'ReturnConsumedCapacity'>>
> & {
  projection?: Projection<INFO, PROJECTION>;
};
export type GetItemReturn<INFO extends DynamoInfo, PROJECTION> = {
  item:
    | (PROJECTION extends null
        ? TypeFromDefinition<INFO['definition']>
        : PROJECTION)
    | undefined;
  consumedCapacity?: ConsumedCapacity;
};

export interface GetExecutor<T extends DynamoInfo, PROJECTION> {
  input: GetItemInput;
  execute(): Promise<GetItemReturn<T, PROJECTION>>;
}

export class DynamoGetter<T extends DynamoInfo> {
  constructor(
    private readonly info: T,
    private readonly config: DynamoConfig,
  ) {}

  async get<PROJECTION = null>(
    keys: PickKeys<T>,
    options: GetItemOptions<T, PROJECTION> = {},
  ): Promise<GetItemReturn<T, PROJECTION>> {
    const getInput = this.getExecutor(keys, options);
    if (this.config.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(getInput.input, null, 2)}`);
    }
    return await getInput.execute();
  }

  getExecutor<PROJECTION = null>(
    keys: PickKeys<T>,
    options: GetItemOptions<T, PROJECTION>,
  ): GetExecutor<T, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    const input = {
      TableName: this.config.tableName,
      Key: keys,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ConsistentRead: options.consistentRead,
      ProjectionExpression: expression,
      ...attributeBuilder.asInput(),
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<GetItemReturn<T, PROJECTION>> {
        const result = await client.get(input).promise();
        return {
          item: result.Item as any,
          consumedCapacity: result.ConsumedCapacity,
        };
      },
    };
  }
}
