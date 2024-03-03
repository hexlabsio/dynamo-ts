import {
  ConsumedCapacity,
  WriteRequest,
} from '@aws-sdk/client-dynamodb/dist-types/models/models_0.js';
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

export class BatchWriteClient<T extends BatchWriteExecutor[]> {
  public readonly input: BatchWriteCommandInput;

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly logStatements: undefined | boolean,
    private readonly executors: T,
  ) {
    const RequestItems = this.executors.reduce((prev, next) => {
      const tables = Object.keys(next.input.RequestItems ?? {});
      return {
        ...prev,
        [tables[0]]: [
          ...(prev[tables[0]] ?? []),
          ...next.input.RequestItems![tables[0]],
        ],
      };
    }, {} as Record<string, any>);
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

  async execute(
    reprocess = false,
    maxRetries = 10,
  ): Promise<{
    consumedCapacity?: ConsumedCapacity[];
    unprocessedItems?: Record<string, WriteRequest[]>;
  }> {
    if (this.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(this.input, null, 2)}`);
    }
    let result = await this.client.batchWrite(this.input);
    let retry = 0;
    let returnType = {
      unprocessedItems: result.UnprocessedItems,
      consumedCapacity: result.ConsumedCapacity,
    };
    while (
      reprocess &&
      Object.keys(returnType.unprocessedItems ?? {}).length > 0 &&
      retry < maxRetries
    ) {
      console.log('Reprocessing', returnType.unprocessedItems);
      await new Promise((resolve) => setTimeout(resolve, 2 ** retry * 10));
      retry = retry + 1;
      result = await this.client.batchWrite({
        ...this.executors[0].input,
        RequestItems: returnType.unprocessedItems!,
      });
      returnType = {
        unprocessedItems: result.UnprocessedItems,
        consumedCapacity: returnType.consumedCapacity
          ? [...returnType.consumedCapacity, ...(result.ConsumedCapacity ?? [])]
          : undefined,
      };
    }
    return returnType as any;
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
