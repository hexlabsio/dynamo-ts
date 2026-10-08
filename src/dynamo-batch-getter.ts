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

type BatchGetRound = {
  /** The executors in this round, by position in the batch */
  executors: number[];
  requestItems: Record<string, KeysAndAttributes>;
};

export class BatchGetClient<T extends BatchGetExecutor<any, any>[]> {
  /** The request for the first round. Batches with several requests for one table need more than one round. */
  public readonly input: BatchGetCommandInput;
  private readonly rounds: BatchGetRound[];

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly executors: T,
  ) {
    // A request can only include each table once, so further requests for a table go in later rounds
    this.rounds = this.executors.reduce((rounds, executor, index) => {
      const requestItems = executor.input.RequestItems as Record<
        string,
        KeysAndAttributes
      >;
      const tables = Object.keys(requestItems ?? {});
      const round = rounds.find((it) =>
        tables.every((table) => !it.requestItems[table]),
      );
      if (round) {
        round.executors.push(index);
        Object.assign(round.requestItems, requestItems);
        return rounds;
      }
      return [
        ...rounds,
        { executors: [index], requestItems: { ...requestItems } },
      ];
    }, [] as BatchGetRound[]);
    this.input = {
      ...this.executors[0].input,
      RequestItems: this.rounds[0]
        .requestItems as BatchGetCommandInput['RequestItems'],
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
    const items: Record<string, any>[][] = this.executors.map(() => []);
    let consumedCapacity: ConsumedCapacity[] | undefined;
    let unprocessedKeys: Record<string, KeysAndAttributes> | undefined;
    for (const round of this.rounds) {
      const result = await this.executeRound(
        round.requestItems,
        reprocess,
        maxRetries,
      );
      round.executors.forEach((index) => {
        const table = Object.keys(
          this.executors[index].input.RequestItems ?? {},
        )[0];
        items[index] = result.responses[table] ?? [];
      });
      if (result.consumedCapacity) {
        consumedCapacity = [
          ...(consumedCapacity ?? []),
          ...result.consumedCapacity,
        ];
      }
      Object.entries(result.unprocessedKeys).forEach(([table, request]) => {
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
      items: items as BatchGetExecutorResult<T>,
      unprocessedKeys: unprocessedKeys ?? {},
      consumedCapacity,
    };
  }

  /**
   * Gets one round of requests, at most one per table, in chunks of up to 100 keys.
   */
  private async executeRound(
    roundItems: Record<string, KeysAndAttributes>,
    reprocess: boolean,
    maxRetries: number,
  ): Promise<{
    responses: Record<string, Record<string, any>[]>;
    consumedCapacity?: ConsumedCapacity[];
    unprocessedKeys: Record<string, KeysAndAttributes>;
  }> {
    const responses: Record<string, Record<string, any>[]> = {};
    let consumedCapacity: ConsumedCapacity[] | undefined;
    const unprocessedKeys: Record<string, KeysAndAttributes> = {};
    for (const chunk of chunkKeys(roundItems, MAX_BATCH_GET_ITEMS)) {
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
        Object.entries(result.Responses ?? {}).forEach(([table, found]) => {
          responses[table] = [...(responses[table] ?? []), ...found];
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
        unprocessedKeys[table] = {
          ...request,
          Keys: [
            ...(unprocessedKeys[table]?.Keys ?? []),
            ...(request.Keys ?? []),
          ],
        };
      });
    }
    return { responses, consumedCapacity, unprocessedKeys };
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
