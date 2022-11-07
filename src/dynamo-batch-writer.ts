import { DynamoDB } from 'aws-sdk';
import BatchWriteItemInput = DynamoDB.DocumentClient.BatchWriteItemInput;
import ItemCollectionMetricsPerTable = DynamoDB.DocumentClient.ItemCollectionMetricsPerTable;
import ConsumedCapacityMultiple = DynamoDB.DocumentClient.ConsumedCapacityMultiple;
import BatchWriteItemRequestMap = DynamoDB.DocumentClient.BatchWriteItemRequestMap;
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoInfo,
  PickKeys,
  TypeFromDefinition,
} from './types';

export type BatchWriteItemOptions<INFO extends DynamoInfo> = CamelCaseKeys<
  Pick<
    BatchWriteItemInput,
    'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'
  >
>;

export type BatchWriteItemReturn<INFO extends DynamoInfo> = {
  itemCollectionMetrics?: ItemCollectionMetricsPerTable;
  consumedCapacity?: ConsumedCapacityMultiple;
};

export interface BatchWriteExecutor<T extends DynamoInfo> {
  input: BatchWriteItemInput;
  execute(): Promise<BatchWriteItemReturn<T>>;
  and<B extends BatchWriteExecutor<any>>(other: B): BatchWriteClient<[this, B]>;
}

export class BatchWriteExecutorHolder<T extends DynamoInfo>
  implements BatchWriteExecutor<T>
{
  constructor(
    private readonly client: DynamoDB.DocumentClient,
    public readonly input: BatchWriteItemInput,
    private readonly logStatements: undefined | boolean,
  ) {}

  async execute(): Promise<BatchWriteItemReturn<T>> {
    if (this.logStatements) {
      console.log(`BatchWriteInput: ${JSON.stringify(this.input, null, 2)}`);
    }
    const result = await this.client.batchWrite(this.input).promise();
    return {
      itemCollectionMetrics: result.ItemCollectionMetrics,
      consumedCapacity: result.ConsumedCapacity,
    };
  }

  and<B extends BatchWriteExecutor<any>>(
    other: B,
  ): BatchWriteClient<[this, B]> {
    return new BatchWriteClient(this.client, this.logStatements, [this, other]);
  }
}

export class BatchWriteClient<T extends BatchWriteExecutor<any>[]> {
  public readonly input: BatchWriteItemInput;

  constructor(
    private readonly client: DynamoDB.DocumentClient,
    private readonly logStatements: undefined | boolean,
    private readonly executors: T,
  ) {
    const RequestItems = this.executors.reduce((prev, next) => {
      const tables = Object.keys(next.input.RequestItems ?? {});
      return {
        ...prev,
        [tables[0]]: [
          ...(prev[tables[0]] ?? []),
          ...next.input.RequestItems[tables[0]],
        ],
      };
    }, {} as Record<string, any>);
    this.input = {
      ...this.executors[0].input,
      RequestItems,
    };
  }

  and<B extends BatchWriteExecutor<any>>(
    other: B,
  ): BatchWriteClient<[...T, B]> {
    return new BatchWriteClient<[...T, B]>(this.client, this.logStatements, [
      ...this.executors,
      other,
    ]);
  }

  async execute(
    reprocess = false,
    maxRetries = 10,
  ): Promise<{
    consumedCapacity?: ConsumedCapacityMultiple;
    unprocessedItems?: BatchWriteItemRequestMap;
  }> {
    if (this.logStatements) {
      console.log(`GetItemInput: ${JSON.stringify(this.input, null, 2)}`);
    }
    let result = await this.client.batchWrite(this.input).promise();
    let retry = 0;
    let returnType = {
      unprocessedItems: result.UnprocessedItems,
      consumedCapacity: result.ConsumedCapacity,
    };
    while (reprocess && Object.keys(returnType.unprocessedItems ?? {}).length > 0 && retry < maxRetries) {
      console.log('Reprocessing', returnType.unprocessedItems);
      await new Promise((resolve) => setTimeout(resolve, 2 ** retry * 10));
      retry = retry + 1;
      result = await this.client
        .batchWrite({
          ...this.executors[0].input,
          RequestItems: returnType.unprocessedItems!,
        })
        .promise();
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

export class DynamoBatchWriter<T extends DynamoInfo> {
  constructor(private readonly config: DynamoConfig) {}

  batchPutExecutor(
    items: TypeFromDefinition<T['definition']>[],
    options: BatchWriteItemOptions<T> = {},
  ): BatchWriteClient<[BatchWriteExecutor<T>]> {
    const input: BatchWriteItemInput = {
      RequestItems: {
        [this.config.tableName]: items.map((item) => ({
          PutRequest: { Item: item },
        })),
      },
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.config.client;
    const logStatements = this.config.logStatements;
    return new BatchWriteClient(client, logStatements, [
      new BatchWriteExecutorHolder(client, input, logStatements),
    ]);
  }

  batchDeleteExecutor(
    keys: PickKeys<T>[],
    options: BatchWriteItemOptions<T> = {},
  ): BatchWriteClient<[BatchWriteExecutor<T>]> {
    const input: BatchWriteItemInput = {
      RequestItems: {
        [this.config.tableName]: keys.map((key) => ({
          DeleteRequest: { Key: key },
        })),
      },
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
    };
    const client = this.config.client;
    const logStatements = this.config.logStatements;
    return new BatchWriteClient(client, logStatements,[
      new BatchWriteExecutorHolder(client, input, logStatements),
    ]);
  }
}
