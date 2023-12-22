import {
  TransactGetItemsCommandInput,
  TransactGetItemsInput,
} from '@aws-sdk/client-dynamodb';
import { ReturnConsumedCapacity } from '@aws-sdk/client-dynamodb/dist-types/models';
import {
  ConsumedCapacity,
  KeysAndAttributes,
} from '@aws-sdk/client-dynamodb/dist-types/models/models_0';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { Projection, ProjectionHandler } from './projector';
import { TableDefinition } from './table-builder/table-definition';
import { CamelCaseKeys, DynamoConfig } from './types';

export type TypeOrProjection<T, PROJECTION> =
  | (PROJECTION extends null ? T : PROJECTION)
  | undefined;

export type TransactGetItemOptions<TableType, PROJECTION> = CamelCaseKeys<
  Pick<KeysAndAttributes, 'ConsistentRead'> &
    Pick<TransactGetItemsInput, 'ReturnConsumedCapacity'>
> & {
  projection?: Projection<TableType, PROJECTION>;
};

export interface TransactGetExecutor<TableTypes extends any[]> {
  input: TransactGetItemsCommandInput;
  execute(options?: {
    returnConsumedCapacity?: ReturnConsumedCapacity;
  }): Promise<{
    items: TableTypes;
    consumedCapacity?: ConsumedCapacity[];
  }>;
  and<B extends any[]>(
    other: TransactGetExecutor<B>,
  ): TransactGetClient<[...TableTypes, ...B]>;
}

export class TransactGetExecutorHolder<TableTypes extends any[]>
  implements TransactGetExecutor<TableTypes>
{
  constructor(
    private readonly client: DynamoDBDocument,
    public readonly input: TransactGetItemsCommandInput,
  ) {}

  /**
   * Execute the transact get request and get the results.
   */
  async execute(
    options: { returnConsumedCapacity?: ReturnConsumedCapacity } = {},
  ): Promise<{
    items: TableTypes;
    consumedCapacity?: ConsumedCapacity[];
  }> {
    return await new TransactGetClient<TableTypes>(this.client, [this]).execute(
      options,
    );
  }

  /**
   * Append another set of requests to apply alongside these requests.
   * @param other
   */
  and<B extends any[]>(
    other: TransactGetExecutor<B>,
  ): TransactGetClient<[...TableTypes, ...B]> {
    return new TransactGetClient(this.client, [this, other]);
  }
}

export class TransactGetClient<TableTypes extends any[]> {
  public readonly input: TransactGetItemsCommandInput;

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly executors: TransactGetExecutor<any>[],
  ) {
    const TransactItems = this.executors.flatMap(
      (it) => it.input.TransactItems ?? [],
    );
    this.input = {
      TransactItems,
    };
  }

  and<B extends any[]>(
    other: TransactGetExecutor<B>,
  ): TransactGetClient<[...TableTypes, ...B]> {
    return new TransactGetClient<[...TableTypes, ...B]>(this.client, [
      ...this.executors,
      other,
    ]);
  }

  async execute(
    options: { returnConsumedCapacity?: ReturnConsumedCapacity } = {},
  ): Promise<{
    items: TableTypes;
    consumedCapacity?: ConsumedCapacity[];
  }> {
    let result = await this.client.transactGet({
      TransactItems: this.input.TransactItems,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
    });
    return {
      items: (result.Responses ?? []).map((it) => it.Item) as any,
      consumedCapacity: result.ConsumedCapacity,
    };
  }
}

export type ReturnTypesFor<K extends any[], T> = K extends [any]
  ? [T]
  : K extends [any, ...infer B]
  ? [T, ...ReturnTypesFor<B, T>]
  : T[];

export class DynamoTransactGetter<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  get<const K extends TableConfig['keys'][], PROJECTION = null>(
    keys: K,
    options: TransactGetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): TransactGetExecutor<
    ReturnTypesFor<K, TypeOrProjection<TableConfig['type'], PROJECTION>>
  > {
    const attributeBuilder = AttributeBuilder.create();
    const expression =
      options.projection &&
      ProjectionHandler.projectionExpressionFor(
        attributeBuilder,
        options.projection,
      );
    const input: TransactGetItemsCommandInput = {
      TransactItems: keys.map((key) => ({
        Get: {
          TableName: this.clientConfig.tableName,
          Key: key,
          ...(options.projection ? { ProjectionExpression: expression } : {}),
          ...attributeBuilder.asInput(),
        },
      })),
    };
    const client = this.clientConfig.client;
    return new TransactGetExecutorHolder(client, input);
  }
}
