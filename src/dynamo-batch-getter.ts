import { ConsumedCapacity, KeysAndAttributes } from '@aws-sdk/client-dynamodb';
import { BatchGetCommandInput, DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder.js';
import { Projection, ProjectionHandler } from './projector.js';
import { TableDefinition } from './table-builder/table-definition.js';
import { CamelCaseKeys, DynamoConfig } from './types/index.js';

export type BatchGetItemOptions<TableType, PROJECTION> = CamelCaseKeys<
  Pick<KeysAndAttributes, 'ConsistentRead'> &
    Pick<BatchGetCommandInput, 'ReturnConsumedCapacity'>
> & {
  projection?: Projection<TableType, PROJECTION>;
};

export type BatchGetItemReturn<TableType, PROJECTION> = {
  items: PROJECTION extends null ? TableType[] : PROJECTION[];
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
    public readonly tableName: string,
    private readonly client: DynamoDBDocument,
    public readonly input: BatchGetCommandInput,
  ) {}

  /**
   * Execute the batch get request and get the results.
   */
  async execute(): Promise<BatchGetItemReturn<TableType, PROJECTION>> {
    const result = await new BatchGetClient(this.client, [this]).execute();
    return {
      items: result.items[0] as any,
      consumedCapacity: result.consumedCapacity?.[0],
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

const MAX_BATCH_GET_ITEMS = 100;

function chunkKeys(
  requestItems: Record<string, KeysAndAttributes>,
  size: number,
): Record<string, KeysAndAttributes>[] {
  const flattened = Object.entries(requestItems).flatMap(([table, request]) =>
    (request.Keys ?? []).map((key) => [table, key] as const),
  );
  const chunks: Record<string, KeysAndAttributes>[] = [];
  for (let i = 0; i < flattened.length; i += size) {
    chunks.push(
      flattened.slice(i, i + size).reduce((chunk, [table, key]) => {
        chunk[table] = {
          ...requestItems[table],
          Keys: [...(chunk[table]?.Keys ?? []), key],
        };
        return chunk;
      }, {} as Record<string, KeysAndAttributes>),
    );
  }
  return chunks;
}

export class BatchGetClient<T extends BatchGetExecutor<any, any>[]> {
  public readonly input: BatchGetCommandInput;

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly executors: T,
  ) {
    const RequestItems = this.executors.reduce((prev, next) => {
      Object.keys(next.input.RequestItems ?? {}).forEach((table) => {
        if (prev[table]) {
          throw new Error(
            `Batch get already contains a request for table '${table}', combine the keys into a single batchGet call instead.`,
          );
        }
      });
      return { ...prev, ...next.input.RequestItems };
    }, {} as Record<string, any>);
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

  /**
   * Executes the batch, splitting it into multiple requests of up to 100 keys if required.
   * @param reprocess - When true, any unprocessed keys will be retried with exponential backoff.
   * @param maxRetries - The maximum number of retries per request when reprocessing.
   */
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
    const responses: Record<string, Record<string, any>[]> = {};
    let consumedCapacity: ConsumedCapacity[] | undefined;
    let unprocessedKeys: Record<string, KeysAndAttributes> | undefined;
    const chunks = chunkKeys(
      (this.input.RequestItems ?? {}) as Record<string, KeysAndAttributes>,
      MAX_BATCH_GET_ITEMS,
    );
    for (const chunk of chunks) {
      let requestItems: Record<string, KeysAndAttributes> | undefined = chunk;
      let retry = 0;
      do {
        if (retry > 0) {
          await new Promise((resolve) =>
            setTimeout(resolve, 2 ** (retry - 1) * 10),
          );
        }
        const result = await this.client.batchGet({
          ...this.input,
          RequestItems: requestItems as BatchGetCommandInput['RequestItems'],
        });
        Object.entries(result.Responses ?? {}).forEach(([table, items]) => {
          responses[table] = [...(responses[table] ?? []), ...items];
        });
        if (result.ConsumedCapacity) {
          consumedCapacity = [
            ...(consumedCapacity ?? []),
            ...result.ConsumedCapacity,
          ];
        }
        requestItems = result.UnprocessedKeys as
          | Record<string, KeysAndAttributes>
          | undefined;
        retry = retry + 1;
      } while (
        reprocess &&
        Object.keys(requestItems ?? {}).length > 0 &&
        retry <= maxRetries
      );
      Object.entries(requestItems ?? {}).forEach(([table, request]) => {
        unprocessedKeys = unprocessedKeys ?? {};
        unprocessedKeys[table] = {
          ...request,
          Keys: [
            ...(unprocessedKeys[table]?.Keys ?? []),
            ...(request.Keys ?? []),
          ],
        };
      });
    }
    return {
      items: tableNameList.map(
        (tableName) => responses[tableName] ?? [],
      ) as BatchGetExecutorResult<T>,
      unprocessedKeys: unprocessedKeys ?? {},
      consumedCapacity,
    };
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
