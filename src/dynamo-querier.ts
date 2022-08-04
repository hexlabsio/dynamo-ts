import { ConsumedCapacity, QueryInput } from 'aws-sdk/clients/dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts, KeyComparisonBuilder, Wrapper } from './comparison';
import { DynamoFilter2 } from './filter';
import { KeyOperation } from './operation';
import { Projection, ProjectionHandler } from './projector';
import { CamelCaseKeys, DynamoConfig, DynamoIndex, DynamoInfo, TypeFromDefinition } from './types';

export type HashCompare<D extends DynamoInfo> = TypeFromDefinition<{ [K in D['partitionKey']]: D['definition'][K] }>
export type SortCompare<D extends DynamoInfo> = D['sortKey'] extends keyof TypeFromDefinition<D['definition']> ? { [K in D['sortKey']]?: (sortKey: KeyComparisonBuilder<TypeFromDefinition<D['definition']>[D['sortKey']]>) => any; } : {};

export type QueryKeys<D extends DynamoInfo> = HashCompare<D> & SortCompare<D>;
export type QuerierInput<D extends DynamoInfo, PROJECTION> = {
  filter?: DynamoFilter2<D>;
  projection?: Projection<D, PROJECTION>;
  next?: string;
} & CamelCaseKeys<Pick<QueryInput, 'Limit' | 'ConsistentRead' | 'ScanIndexForward' | 'ReturnConsumedCapacity' | 'ExpressionAttributeNames' | 'ExpressionAttributeValues'>>

export type QuerierReturn<D extends DynamoInfo, PROJECTION = null> = {
  member: PROJECTION extends null ? TypeFromDefinition<D['definition']>[] : PROJECTION[];
  next?: string;
  consumedCapacity?: ConsumedCapacity;
  count?: number;
  scannedCount?: number;
}

export interface QueryExecutor<D extends DynamoInfo, PROJECTION> {
  input: QueryInput;
  execute: () => Promise<QuerierReturn<D, PROJECTION>>;
}

export class DynamoQuerier<D extends DynamoInfo = any, I extends Record<string, DynamoIndex> = {}> {
  constructor(private readonly info: D, private readonly config: DynamoConfig) {}

  private keyExpression(keys: QueryKeys<D>, attributeBuilder: AttributeBuilder): string {
    const partitionKey = this.info.partitionKey as keyof QueryKeys<D>;
    const sortKey = this.info.sortKey as keyof QueryKeys<D>;
    attributeBuilder.addNames(partitionKey as string);
    const hashValue = keys[partitionKey];
    const valueKey = attributeBuilder.addValue(hashValue);
    const expression = `${attributeBuilder.nameFor(partitionKey as string)} = ${valueKey}`;
    if (sortKey && keys[sortKey]) {
      const keyOperation = new KeyOperation(sortKey as string, new Wrapper(attributeBuilder),);
      (keys[sortKey] as any)(keyOperation);
      return `${expression} AND ${keyOperation.wrapper.expression}`;
    }
    return expression;
  }

  query<PROJECTION = null>(keys: QueryKeys<D>, options: QuerierInput<D, PROJECTION> = {}): Promise<QuerierReturn<D, PROJECTION>> {
    const executor = this.queryExecutor(keys, options);
    if (this.config.logStatements) {
      console.log(`QueryInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    return executor.execute();
  }

  queryExecutor<PROJECTION = null>(keys: QueryKeys<D>, options: QuerierInput<D, PROJECTION> = {}): QueryExecutor<D, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyExpression(keys, attributeBuilder);
    const filterPart = options.filter && filterParts(this.info, attributeBuilder, options.filter);
    const projection = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    const input: QueryInput = {
      TableName: this.config.tableName,
      ...(this.config.indexName ? { IndexName: this.config.indexName } : {}),
      ...{ KeyConditionExpression: keyExpression },
      ...(options.filter ? { FilterExpression: filterPart } : {}),
      ProjectionExpression: projection,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ScanIndexForward: options.scanIndexForward,
      ConsistentRead: options.consistentRead,
      Limit: options.limit,
      ...attributeBuilder.asInput(options),
      ...(options.next
        ? {
          ExclusiveStartKey: JSON.parse(
            Buffer.from(options.next, 'base64').toString('ascii'),
          ),
        }
        : {}),
    };
    const client = this.config.client;
    return {
      input,
      execute: async () => {
        const result = await client.query(input).promise();
        return {
          member: (result.Items ?? []) as any,
          next: result.LastEvaluatedKey ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString('base64') : undefined,
          consumedCapacity: result.ConsumedCapacity,
          count: result.Count,
          scannedCount: result.ScannedCount
        }
      }
    }
  }
}