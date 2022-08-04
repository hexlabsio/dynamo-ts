import { DynamoDB } from 'aws-sdk';
import BatchGetItemInput = DynamoDB.DocumentClient.BatchGetItemInput;
import ConsumedCapacity = DynamoDB.DocumentClient.ConsumedCapacity;
import KeysAndAttributes = DynamoDB.DocumentClient.KeysAndAttributes;
import BatchGetRequestMap = DynamoDB.DocumentClient.BatchGetRequestMap;
import ConsumedCapacityMultiple = DynamoDB.DocumentClient.ConsumedCapacityMultiple;
import { AttributeBuilder } from './attribute-builder';
import { Projection, ProjectionHandler } from './projector';
import { CamelCaseKeys, DynamoConfig, DynamoInfo, PickKeys, TypeFromDefinition } from './types';

export type BatchGetItemOptions<INFO extends DynamoInfo, PROJECTION> = CamelCaseKeys<Pick<KeysAndAttributes, 'ConsistentRead'> & Pick<BatchGetItemInput, 'ReturnConsumedCapacity'>> & {
  projection?: Projection<INFO, PROJECTION>;
}

export type BatchGetItemReturn<INFO extends DynamoInfo, PROJECTION> = {
  items: PROJECTION extends null ? TypeFromDefinition<INFO['definition']>[] : PROJECTION[];
  consumedCapacity?: ConsumedCapacity;
}

export interface BatchGetExecutor<T extends DynamoInfo, PROJECTION> {
  input: BatchGetItemInput;
  execute(): Promise<BatchGetItemReturn<T, PROJECTION>>
  and<B extends BatchGetExecutor<any, any>>(other: B): BatchGetClient<[this, B]>;
}

export class BatchGetExecutorHolder<T extends DynamoInfo, PROJECTION> implements BatchGetExecutor<T, PROJECTION> {
  constructor(private readonly tableName: string, private readonly client: DynamoDB.DocumentClient, public readonly input: BatchGetItemInput) {}

  async execute(): Promise<BatchGetItemReturn<T, PROJECTION>> {
    const result = await this.client.batchGet(this.input).promise();
    return {
      items: result.Responses?.[this.tableName] ?? [] as any,
      consumedCapacity: result.ConsumedCapacity?.[0]
    }
  }

  and<B extends BatchGetExecutor<any, any>>(other: B): BatchGetClient<[this, B]> {
    return new BatchGetClient(this.client, [this, other]);
  }
}

type BatchGetExecutorReturn<A> =
  A extends BatchGetExecutor<infer T, infer P>
    ? BatchGetItemReturn<T, P>['items']
    : unknown[];

type BatchGetExecutorResult<T extends BatchGetExecutor<any, any>[]> =
  T extends [infer A]
    ? [BatchGetExecutorReturn<A>]
    : T extends [infer A, ...infer Tail]
      ? ([BatchGetExecutorReturn<A>, ...(Tail extends BatchGetExecutor<any, any>[] ? BatchGetExecutorResult<Tail> : [])])
      : T extends [infer A]
        ? [BatchGetExecutorReturn<A>]
        : never;

export class BatchGetClient<T extends BatchGetExecutor<any, any>[]> {

  public readonly input: BatchGetItemInput;

  constructor(private readonly client: DynamoDB.DocumentClient, private readonly executors: T) {
    const RequestItems = this.executors.reduce((prev, next) => ({...prev, ...next.input.RequestItems}), {});
    this.input = {
      ...this.executors[0].input,
      RequestItems
    }
  }

  and<B extends BatchGetExecutor<any, any>>(other: B): BatchGetClient<[...T, B]> {
    return new BatchGetClient<[...T, B]>(this.client, [...this.executors, other]);
  }

  async execute(reprocess = false): Promise<{ items: BatchGetExecutorResult<T>, consumedCapacity?: ConsumedCapacityMultiple, unprocessedKeys?: BatchGetRequestMap }> {
    const tableNameList = this.executors.map(it => Object.keys(it.input.RequestItems)[0]);
    let result = await this.client.batchGet(this.input).promise();
    let returnType = {
      items: tableNameList.map(tableName => result.Responses?.[tableName] ?? []),
      unprocessedKeys: result.UnprocessedKeys,
      consumedCapacity: result.ConsumedCapacity
    }
    while(reprocess && !!returnType.unprocessedKeys) {
      result = await this.client.batchGet({
        ...this.executors[0].input,
        RequestItems: returnType.unprocessedKeys
      }).promise();
      returnType = {
        items: tableNameList.map(tableName => {
          const index = tableNameList.findIndex(it => it === tableName);
          return [...returnType.items[index] ?? [], ...result.Responses?.[tableName] ?? []]
        }),
        unprocessedKeys: result.UnprocessedKeys,
        consumedCapacity: returnType.consumedCapacity ? [...returnType.consumedCapacity, ...result.ConsumedCapacity ?? []] : undefined
      }
    }
    return returnType as any;
  }
}

export class DynamoBatchGetter<T extends DynamoInfo> {
  constructor(private readonly info: T, private readonly config: DynamoConfig) {}

  batchGetExecutor<PROJECTION = null>(keys: PickKeys<T>[], options: BatchGetItemOptions<T, PROJECTION> = {}): BatchGetExecutor<T, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    const input = {
      RequestItems: {
        [this.config.tableName]: {
          Keys: keys,
          ProjectionExpression: expression,
          ...attributeBuilder.asInput(),
        },
      },
      ReturnConsumedCapacity: options.returnConsumedCapacity
    };
    const client = this.config.client;
    const tableName = this.config.tableName;
    return new BatchGetExecutorHolder(tableName, client, input);
  }
}