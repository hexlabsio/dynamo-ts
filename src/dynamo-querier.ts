import { QueryCommandInput, QueryCommandOutput } from '@aws-sdk/lib-dynamodb';
import { NativeAttributeValue } from '@aws-sdk/util-dynamodb';

import { AttributeBuilder } from './attribute-builder';
import { filterParts, KeyComparisonBuilder, Wrapper } from './comparison';
import { DynamoFilter } from './filter';
import { KeyOperation } from './operation';
import { Projection, ProjectionHandler } from './projector';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoDefinition,
  DynamoIndex,
  DynamoInfo,
  TypeFromDefinition,
} from './types';

export type HashCompare<D extends DynamoInfo> = TypeFromDefinition<{
  [K in D['partitionKey']]: D['definition'][K];
}>;
export type SortCompare<D extends DynamoInfo> =
  D['sortKey'] extends keyof TypeFromDefinition<D['definition']>
    ? {
        [K in D['sortKey']]?: (
          sortKey: KeyComparisonBuilder<
            TypeFromDefinition<D['definition']>[D['sortKey']]
          >,
        ) => any;
      }
    : {};

export type QueryKeys<D extends DynamoInfo> = HashCompare<D> & SortCompare<D>;
export type QuerierInput<D extends DynamoInfo, PROJECTION> = {
  filter?: DynamoFilter<D>;
  projection?: Projection<D, PROJECTION>;
  next?: string;
} & Partial<
  CamelCaseKeys<
    Pick<
      QueryCommandInput,
      | 'Limit'
      | 'ConsistentRead'
      | 'ScanIndexForward'
      | 'ReturnConsumedCapacity'
      | 'ExpressionAttributeNames'
      | 'ExpressionAttributeValues'
    >
  >
>;
export type QueryAllInput<D extends DynamoInfo, PROJECTION> = QuerierInput<
  D,
  PROJECTION
>;

export type QuerierReturn<D extends DynamoInfo, PROJECTION = null> = {
  member: PROJECTION extends null
    ? TypeFromDefinition<D['definition']>[]
    : PROJECTION[];
  next?: string;
  consumedCapacity?: QueryCommandOutput['ConsumedCapacity'];
  count?: number;
  scannedCount?: number;
};
export type ParentKeys<D extends DynamoDefinition> = {
  partitionKey: keyof D;
  sortKey?: keyof D;
};

export interface QueryExecutor<D extends DynamoInfo, PROJECTION> {
  input: QueryCommandInput;
  execute: () => Promise<QuerierReturn<D, PROJECTION>>;
}

export class DynamoQuerier<
  D extends DynamoInfo = any,
  I extends Record<string, DynamoIndex> = {},
> {
  constructor(
    private readonly info: D,
    private readonly config: DynamoConfig,
    private readonly parentKeys?: ParentKeys<D['definition']>,
  ) {}

  private keyExpression(
    keys: QueryKeys<D>,
    attributeBuilder: AttributeBuilder,
  ): string {
    const partitionKey = this.info.partitionKey as keyof QueryKeys<D>;
    const sortKey = this.info.sortKey as keyof QueryKeys<D>;
    attributeBuilder.addNames(partitionKey as string);
    const hashValue = keys[partitionKey];
    const valueKey = attributeBuilder.addValue(hashValue);
    const expression = `${attributeBuilder.nameFor(
      partitionKey as string,
    )} = ${valueKey}`;
    if (sortKey && typeof keys[sortKey] === 'function') {
      const keyOperation = new KeyOperation(
        sortKey as string,
        new Wrapper(attributeBuilder),
      );
      (keys[sortKey] as any)(keyOperation);
      return `${expression} AND ${keyOperation.wrapper.expression}`;
    }
    return expression;
  }

  query<PROJECTION = null>(
    keys: QueryKeys<D>,
    options: QuerierInput<D, PROJECTION> = {},
  ): Promise<QuerierReturn<D, PROJECTION>> {
    const executor = this.queryExecutor(keys, options);
    if (this.config.logStatements) {
      console.log(`QueryInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    return executor.execute();
  }

  async queryAll<PROJECTION = null>(
    keys: QueryKeys<D>,
    options: QueryAllInput<D, PROJECTION> = {},
  ): Promise<QuerierReturn<D, PROJECTION>> {
    const executor = this.queryAllExecutor(keys, options);
    if (this.config.logStatements) {
      console.log(`QueryAllInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    return executor.execute();
  }

  queryExecutor<PROJECTION = null>(
    keys: QueryKeys<D>,
    options: QuerierInput<D, PROJECTION> = {},
  ): QueryExecutor<D, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyExpression(keys, attributeBuilder);
    const filterPart =
      options.filter &&
      filterParts(this.info, attributeBuilder, options.filter);
    const projection = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    const input: QueryCommandInput = {
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
              Buffer.from(options.next, 'base64').toString(),
            ),
          }
        : {}),
    };
    const client = this.config.client;
    return {
      input,
      execute: async () => {
        const result = await client.query(input);
        return {
          member: (result.Items ?? []) as any,
          next: result.LastEvaluatedKey
            ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
                'base64',
              )
            : undefined,
          consumedCapacity: result.ConsumedCapacity,
          count: result.Count,
          scannedCount: result.ScannedCount,
        };
      },
    };
  }

  queryAllExecutor<PROJECTION = null>(
    keys: QueryKeys<D>,
    options: QueryAllInput<D, PROJECTION> = {},
  ): QueryExecutor<D, PROJECTION> {
    return new QueryAllExecutor(
      this.info,
      this.config,
      keys,
      options,
      (keys, builder) => this.keyExpression(keys, builder),
      this.parentKeys,
    );
  }
}

class QueryAllExecutor<D extends DynamoInfo, PROJECTION>
  implements QueryExecutor<D, PROJECTION>
{

  input: QueryCommandInput;
  constructor(
    private readonly info: D,
    private readonly config: DynamoConfig,
    private readonly keys: QueryKeys<D>,
    private readonly options: QueryAllInput<D, PROJECTION>,
    private readonly createKeyExpression: (
      keys: QueryKeys<D>,
      builder: AttributeBuilder,
    ) => string,
    readonly parentKeys?: ParentKeys<D['definition']>,
    private readonly attributeBuilder = AttributeBuilder.create(),
    private readonly projectionWithEnrichedKeys = ProjectionHandler.projectionWithKeysFor(
      attributeBuilder,
      info,
      parentKeys?.partitionKey ?? null,
      parentKeys?.sortKey ?? null,
      options.projection,
    ),
  ) {
    this.input = {
      TableName: this.config.tableName,
      ...(this.config.indexName ? { IndexName: this.config.indexName } : {}),
      ...{
        KeyConditionExpression: this.createKeyExpression(
          this.keys,
          this.attributeBuilder,
        ),
      },
      ...(this.options.filter
        ? {
          FilterExpression: filterParts(
            this.info,
            this.attributeBuilder,
            this.options.filter,
          ),
        }
        : {}),
      ProjectionExpression: this.projectionWithEnrichedKeys[0],
      ReturnConsumedCapacity: this.options.returnConsumedCapacity,
      ScanIndexForward: this.options.scanIndexForward,
      ConsistentRead: this.options.consistentRead,
      ...this.attributeBuilder.asInput(this.options),
      ...(this.options.next
        ? {
          ExclusiveStartKey: JSON.parse(
            Buffer.from(this.options.next, 'base64').toString(),
          ),
        }
        : {}),
    };
  }

  private buildNext(
    lastItem: Record<string, NativeAttributeValue>,
    keyFields: {
      partitionKey: string;
      sortKey?: string;
      parentPartitionKey?: string;
      parentSortKey?: string;
    },
  ): any {
    const nextKey = {
      [keyFields.partitionKey]: lastItem[keyFields.partitionKey],
    };
    if (keyFields.sortKey)
      nextKey[keyFields.sortKey] = lastItem[keyFields.sortKey];
    if (keyFields.parentPartitionKey)
      nextKey[keyFields.parentPartitionKey] =
        lastItem[keyFields.parentPartitionKey];
    if (keyFields.parentSortKey)
      nextKey[keyFields.parentSortKey] = lastItem[keyFields.parentSortKey];
    return nextKey;
  }

  private removeFields(
    lastItems: Record<string, NativeAttributeValue>[],
    enrichedFields: string[],
  ): void {
    lastItems.forEach((lastItem) =>
      enrichedFields.forEach((field) => delete lastItem[field]),
    );
  }

  private async _recQuery(
    queryInput: QueryCommandInput,
    keyFields: {
      partitionKey: string;
      sortKey?: string;
      parentPartitionKey?: string;
      parentSortKey?: string;
    },
    enrichedFields?: string[],
    queryLimit?: number,
    accumulation: QueryCommandOutput['Items'] = [],
    accumulationCount?: number,
  ): Promise<{
    Items: QueryCommandOutput['Items'];
    LastEvaluatedKey?: string;
  }> {
    const res = await this.config.client.query(queryInput);

    const resLength = res?.Items?.length ?? 0;
    const accLength = accumulationCount ?? 0;
    const updatedAccLength = accLength + resLength;
    const limit = queryLimit ?? 0;

    if (limit > 0 && limit <= updatedAccLength) {
      const nextKey = this.buildNext(
        res.Items![limit - accLength - 1],
        keyFields,
      );
      const accumulatedResults = [
        ...res.Items!.slice(0, limit - accLength),
        ...accumulation,
      ];
      if (enrichedFields) {
        this.removeFields(accumulatedResults, enrichedFields);
      }
      return {
        Items: accumulatedResults,
        LastEvaluatedKey: nextKey,
      };
    } else if (!res.LastEvaluatedKey) {
      const accumulatedResults = [...(res.Items ?? []), ...accumulation];
      if (enrichedFields) {
        this.removeFields(accumulatedResults, enrichedFields);
      }
      return {
        Items: accumulatedResults,
      };
    } else {
      return await this._recQuery(
        { ...queryInput, ExclusiveStartKey: res.LastEvaluatedKey },
        keyFields,
        enrichedFields,
        queryLimit,
        [...accumulation, ...(res.Items ?? [])],
        updatedAccLength,
      );
    }
  }



  async execute(): Promise<QuerierReturn<D, PROJECTION>> {
    const result = await this._recQuery(
      this.input,
      {
        partitionKey: this.info.partitionKey as string,
        sortKey: this.info.sortKey as string | undefined,
        parentPartitionKey: this.parentKeys?.partitionKey as string | undefined,
        parentSortKey: this.parentKeys?.sortKey as string | undefined,
      },
      this.projectionWithEnrichedKeys[1],
      this.options.limit,
    );

    return {
      member: (result.Items ?? []) as any,
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey)).toString(
            'base64',
          )
        : undefined,
      count: result.Items?.length ?? 0,
      // consumedCapacity: result.ConsumedCapacity,
      // scannedCount: result.ScannedCount,
    };
  }
}
