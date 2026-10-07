import { ConsumedCapacity, WriteRequest } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommandInput,
  BatchWriteCommandOutput,
  DynamoDBDocument,
} from '@aws-sdk/lib-dynamodb';
import { TableDefinition } from './table-builder/table-definition.js';
import { CamelCaseKeys, DynamoConfig } from './types/index.js';

export type BatchWriteItemOptions = CamelCaseKeys<
  Pick<
    BatchWriteCommandInput,
    'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'
  >
>;

export type BatchWriteItemReturn = {
  itemCollectionMetrics?: BatchWriteCommandOutput['ItemCollectionMetrics'];
  consumedCapacity?: ConsumedCapacity[];
};

export interface BatchWriteExecutor {
  input: BatchWriteCommandInput;
  execute(): Promise<BatchWriteItemReturn>;
  and<B extends BatchWriteExecutor>(other: B): BatchWriteClient<[this, B]>;
}

export class BatchWriteExecutorHolder<TableConfig extends TableDefinition>
  implements BatchWriteExecutor
{
  constructor(
    private readonly client: DynamoDBDocument,
    public readonly input: BatchWriteCommandInput,
    private readonly logStatements: undefined | boolean,
  ) {}

  async execute(): Promise<BatchWriteItemReturn> {
    if (this.logStatements) {
      console.log(`BatchWriteInput: ${JSON.stringify(this.input, null, 2)}`);
    }
    const result = await this.client.batchWrite(this.input);
    return {
      itemCollectionMetrics: result.ItemCollectionMetrics,
      consumedCapacity: result.ConsumedCapacity,
    };
  }

  and<B extends BatchWriteExecutor>(other: B): BatchWriteClient<[this, B]> {
    return new BatchWriteClient(this.client, this.logStatements, [this, other]);
  }
}

const MAX_BATCH_WRITE_ITEMS = 25;

function chunkWriteRequests(
  requestItems: Record<string, WriteRequest[]>,
  size: number,
): Record<string, WriteRequest[]>[] {
  const flattened = Object.entries(requestItems).flatMap(([table, requests]) =>
    requests.map((request) => [table, request] as const),
  );
  const chunks: Record<string, WriteRequest[]>[] = [];
  for (let i = 0; i < flattened.length; i += size) {
    chunks.push(
      flattened.slice(i, i + size).reduce((chunk, [table, request]) => {
        chunk[table] = [...(chunk[table] ?? []), request];
        return chunk;
      }, {} as Record<string, WriteRequest[]>),
    );
  }
  return chunks;
}

export class BatchWriteClient<T extends BatchWriteExecutor[]> {
  public readonly input: BatchWriteCommandInput;

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly logStatements: undefined | boolean,
    private readonly executors: T,
  ) {
    const RequestItems = this.executors.reduce((prev, next) => {
      Object.entries(next.input.RequestItems ?? {}).forEach(
        ([table, requests]) => {
          prev[table] = [...(prev[table] ?? []), ...requests];
        },
      );
      return prev;
    }, {} as Record<string, any[]>);
    this.input = {
      ...this.executors[0].input,
      RequestItems,
    };
  }

  and<B extends BatchWriteExecutor>(other: B): BatchWriteClient<[...T, B]> {
    return new BatchWriteClient<[...T, B]>(this.client, this.logStatements, [
      ...this.executors,
      other,
    ]);
  }

  /**
   * Executes the batch, splitting it into multiple requests of up to 25 items if required.
   * @param reprocess - When true, any unprocessed items will be retried with exponential backoff.
   * @param maxRetries - The maximum number of retries per request when reprocessing.
   */
  async execute(
    reprocess = false,
    maxRetries = 10,
  ): Promise<{
    consumedCapacity?: ConsumedCapacity[];
    unprocessedItems?: Record<string, WriteRequest[]>;
  }> {
    if (this.logStatements) {
      console.log(`BatchWriteInput: ${JSON.stringify(this.input, null, 2)}`);
    }
    const chunks = chunkWriteRequests(
      (this.input.RequestItems ?? {}) as Record<string, WriteRequest[]>,
      MAX_BATCH_WRITE_ITEMS,
    );
    let consumedCapacity: ConsumedCapacity[] | undefined;
    let unprocessedItems: Record<string, WriteRequest[]> | undefined;
    for (const chunk of chunks) {
      const result = await this.executeChunk(chunk, reprocess, maxRetries);
      if (result.consumedCapacity) {
        consumedCapacity = [
          ...(consumedCapacity ?? []),
          ...result.consumedCapacity,
        ];
      }
      Object.entries(result.unprocessedItems ?? {}).forEach(
        ([table, requests]) => {
          unprocessedItems = unprocessedItems ?? {};
          unprocessedItems[table] = [
            ...(unprocessedItems[table] ?? []),
            ...requests,
          ];
        },
      );
    }
    return {
      unprocessedItems: unprocessedItems ?? {},
      consumedCapacity,
    };
  }

  private async executeChunk(
    requestItems: Record<string, WriteRequest[]>,
    reprocess: boolean,
    maxRetries: number,
  ): Promise<{
    consumedCapacity?: ConsumedCapacity[];
    unprocessedItems?: Record<string, WriteRequest[]>;
  }> {
    let result = await this.client.batchWrite({
      ...this.input,
      RequestItems: requestItems as BatchWriteCommandInput['RequestItems'],
    });
    let retry = 0;
    let returnType = {
      unprocessedItems: result.UnprocessedItems as
        | Record<string, WriteRequest[]>
        | undefined,
      consumedCapacity: result.ConsumedCapacity,
    };
    while (
      reprocess &&
      Object.keys(returnType.unprocessedItems ?? {}).length > 0 &&
      retry < maxRetries
    ) {
      if (this.logStatements) {
        console.log('Reprocessing', returnType.unprocessedItems);
      }
      await new Promise((resolve) => setTimeout(resolve, 2 ** retry * 10));
      retry = retry + 1;
      result = await this.client.batchWrite({
        ...this.input,
        RequestItems:
          returnType.unprocessedItems as BatchWriteCommandInput['RequestItems'],
      });
      returnType = {
        unprocessedItems: result.UnprocessedItems as
          | Record<string, WriteRequest[]>
          | undefined,
        consumedCapacity: returnType.consumedCapacity
          ? [...returnType.consumedCapacity, ...(result.ConsumedCapacity ?? [])]
          : undefined,
      };
    }
    return returnType;
  }
}

export class DynamoBatchWriter<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  batchPutExecutor(
    items: TableConfig['type'][],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    const input: BatchWriteCommandInput = {
      RequestItems: {
        [this.clientConfig.tableName]: items.map((item) => ({
          PutRequest: { Item: item },
        })),
      },
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.clientConfig.client;
    const logStatements = this.clientConfig.logStatements;
    return new BatchWriteClient(client, logStatements, [
      new BatchWriteExecutorHolder(client, input, logStatements),
    ]);
  }

  batchDeleteExecutor(
    keys: TableConfig['keys'][],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    const input: BatchWriteCommandInput = {
      RequestItems: {
        [this.clientConfig.tableName]: keys.map((key) => ({
          DeleteRequest: { Key: key },
        })),
      },
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.clientConfig.client;
    const logStatements = this.clientConfig.logStatements;
    return new BatchWriteClient(client, logStatements, [
      new BatchWriteExecutorHolder(client, input, logStatements),
    ]);
  }
}
