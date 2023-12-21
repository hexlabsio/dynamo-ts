import {
  ConsumedCapacity,
  KeysAndAttributes,
} from '@aws-sdk/client-dynamodb/dist-types/models/models_0';
import { BatchGetCommandInput, DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { Projection, ProjectionHandler } from './projector';
import { TableDefinition } from './table-builder/table-definition';
import { CamelCaseKeys, DynamoConfig } from './types';

export type BatchGetItemOptions<TableType, PROJECTION> = CamelCaseKeys<
  Pick<KeysAndAttributes, 'ConsistentRead'> &
    Pick<BatchGetCommandInput, 'ReturnConsumedCapacity'>
> & {
  projection?: Projection<TableType, PROJECTION>;
};

export type BatchGetItemReturn<TableType, PROJECTION> = {
  items: PROJECTION extends null ? TableType : PROJECTION[];
  consumedCapacity?: ConsumedCapacity;
};

export interface BatchGetExecutor<TableType, PROJECTION> {
  input: BatchGetCommandInput;
  execute(): Promise<BatchGetItemReturn<TableType, PROJECTION>>;
  and<B extends BatchGetExecutor<any, any>>(
    other: B,
  ): BatchGetClient<[this, B]>;
}

export class BatchGetExecutorHolder<TableType, PROJECTION>
  implements BatchGetExecutor<TableType, PROJECTION>
{
  constructor(
    private readonly tableName: string,
    private readonly client: DynamoDBDocument,
    public readonly input: BatchGetCommandInput,
  ) {}

  /**
   * Execute the batch get request and get the results.
   */
  async execute(): Promise<BatchGetItemReturn<TableType, PROJECTION>> {
    const result = await this.client.batchGet(this.input);
    return {
      items: result.Responses?.[this.tableName] ?? ([] as any),
      consumedCapacity: result.ConsumedCapacity?.[0],
    };
  }

  /**
   * Append another set of requests to apply alongside these requests.
   * @param other
   */
  and<B extends BatchGetExecutor<any, any>>(
    other: B,
  ): BatchGetClient<[this, B]> {
    return new BatchGetClient(this.client, [this, other]);
  }
}

type BatchGetExecutorReturn<A> = A extends BatchGetExecutor<infer T, infer P>
  ? BatchGetItemReturn<T, P>['items']
  : unknown[];

type BatchGetExecutorResult<T extends BatchGetExecutor<any, any>[]> =
  T extends [infer A]
    ? [BatchGetExecutorReturn<A>]
    : T extends [infer A, ...infer Tail]
    ? [
        BatchGetExecutorReturn<A>,
        ...(Tail extends BatchGetExecutor<any, any>[]
          ? BatchGetExecutorResult<Tail>
          : []),
      ]
    : T extends [infer A]
    ? [BatchGetExecutorReturn<A>]
    : never;

export class BatchGetClient<T extends BatchGetExecutor<any, any>[]> {
  public readonly input: BatchGetCommandInput;

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly executors: T,
  ) {
    const RequestItems = this.executors.reduce(
      (prev, next) => ({ ...prev, ...next.input.RequestItems }),
      {},
    );
    this.input = {
      ...this.executors[0].input,
      RequestItems,
    };
  }

  and<B extends BatchGetExecutor<any, any>>(
    other: B,
  ): BatchGetClient<[...T, B]> {
    return new BatchGetClient<[...T, B]>(this.client, [
      ...this.executors,
      other,
    ]);
  }

  async execute(
    reprocess = false,
    maxRetries = 10,
  ): Promise<{
    items: BatchGetExecutorResult<T>;
    consumedCapacity?: ConsumedCapacity[];
    unprocessedKeys?: Record<string, KeysAndAttributes>;
  }> {
    const tableNameList = this.executors.map(
      (it) => Object.keys(it.input.RequestItems!)[0],
    );
    let result = await this.client.batchGet(this.input);
    let retry = 0;
    let returnType = {
      items: tableNameList.map(
        (tableName) => result.Responses?.[tableName] ?? [],
      ),
      unprocessedKeys: result.UnprocessedKeys,
      consumedCapacity: result.ConsumedCapacity,
    };
    while (reprocess && !!returnType.unprocessedKeys && retry < maxRetries) {
      await new Promise((resolve) => setTimeout(resolve, 2 ** retry * 10));
      retry = retry + 1;
      result = await this.client.batchGet({
        ...this.executors[0].input,
        RequestItems: returnType.unprocessedKeys,
      });
      returnType = {
        items: tableNameList.map((tableName) => {
          const index = tableNameList.findIndex((it) => it === tableName);
          return [
            ...(returnType.items[index] ?? []),
            ...(result.Responses?.[tableName] ?? []),
          ];
        }),
        unprocessedKeys: result.UnprocessedKeys,
        consumedCapacity: returnType.consumedCapacity
          ? [...returnType.consumedCapacity, ...(result.ConsumedCapacity ?? [])]
          : undefined,
      };
    }
    return returnType as any;
  }
}

export class DynamoBatchGetter<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  batchGetExecutor<PROJECTION = null>(
    keys: TableConfig['keys'][],
    options: BatchGetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): BatchGetExecutor<TableConfig['type'], PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression =
      options.projection &&
      ProjectionHandler.projectionExpressionFor(
        attributeBuilder,
        options.projection,
      );
    const input = {
      RequestItems: {
        [this.clientConfig.tableName]: {
          Keys: keys,
          ...(options.projection ? { ProjectionExpression: expression } : {}),
          ...attributeBuilder.asInput(),
        },
      },
      ReturnConsumedCapacity: options.returnConsumedCapacity,
    };
    const client = this.clientConfig.client;
    const tableName = this.clientConfig.tableName;
    return new BatchGetExecutorHolder(tableName, client, input);
  }
}
