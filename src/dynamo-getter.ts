import { ConsumedCapacity } from '@aws-sdk/client-dynamodb/dist-types/models/models_0';
import { GetCommandInput } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { Projection, ProjectionHandler } from './projector';
import { TableDefinition } from './table-builder/table-definition';
import { CamelCaseKeys, DynamoConfig } from './types';

export type GetItemOptions<TableType, PROJECTION> = Partial<
  CamelCaseKeys<
    Pick<GetCommandInput, 'ConsistentRead' | 'ReturnConsumedCapacity'>
  >
> & {
  projection?: Projection<TableType, PROJECTION>;
};
export type GetItemReturn<TableType, PROJECTION> = {
  item: (PROJECTION extends null ? TableType : PROJECTION) | undefined;
  consumedCapacity?: ConsumedCapacity;
};

export interface GetExecutor<TableType, PROJECTION> {
  input: GetCommandInput;
  execute(): Promise<GetItemReturn<TableType, PROJECTION>>;
}

export class DynamoGetter<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  async get<PROJECTION = null>(
    keys: TableConfig['keys'],
    options: GetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<GetItemReturn<TableConfig['type'], PROJECTION>> {
    const getInput = this.getExecutor(keys, options);
    if (this.clientConfig.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(getInput.input, null, 2)}`);
    }
    return await getInput.execute();
  }

  getExecutor<PROJECTION = null>(
    keys: TableConfig['keys'],
    options: GetItemOptions<TableConfig['type'], PROJECTION>,
  ): GetExecutor<TableConfig['type'], PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression =
      options.projection &&
      ProjectionHandler.projectionExpressionFor(
        attributeBuilder,
        options.projection,
      );
    const input = {
      TableName: this.clientConfig.tableName,
      Key: keys,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ConsistentRead: options.consistentRead,
      ...(expression ? { ProjectionExpression: expression } : {}),
      ...attributeBuilder.asInput(),
    };
    const client = this.clientConfig.client;
    return {
      input,
      async execute(): Promise<GetItemReturn<TableConfig['type'], PROJECTION>> {
        const result = await client.get(input);
        return {
          item: result.Item as any,
          consumedCapacity: result.ConsumedCapacity,
        };
      },
    };
  }
}
