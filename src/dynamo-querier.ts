import { QueryCommandInput, QueryCommandOutput } from '@aws-sdk/lib-dynamodb';
import { NativeAttributeValue } from '@aws-sdk/util-dynamodb';

import { AttributeBuilder } from './attribute-builder.js';
import { filterParts, KeyComparisonBuilder, Wrapper } from './comparison.js';
import { KeyOperation } from './operation.js';
import { Projected, Select, selectExpression } from './projector.js';
import {
  DynamoTableKeyConfig,
  TableDefinition,
} from './table-builder/table-definition.js';
import { CamelCaseKeys } from './types/camel-case.js';
import { DynamoConfig } from './types/dynamo-config.js';
import { DynamoFilter } from './types/filter.js';

export type KeyCompare<
  TableType,
  KEYS extends DynamoTableKeyConfig<any>,
> = KEYS extends { sortKey: infer S; partitionKey: infer K }
  ? Pick<TableType, K & keyof TableType> & {
      [K in S & string]?: K extends keyof TableType
        ? (sortKey: KeyComparisonBuilder<TableType[K]>) => any
        : never;
    }
  : Pick<TableType, KEYS['partitionKey'] & keyof TableType>;

export type QuerierInput<TableType, PROJECTION> = {
  filter?: DynamoFilter<TableType>;
  /** Only read these attribute paths */
  select?: Select<TableType, PROJECTION>;
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
export type QueryAllInput<TableType, PROJECTION> = QuerierInput<
  TableType,
  PROJECTION
>;

export type QuerierReturn<TableType, PROJECTION = null> = {
  member: Projected<TableType, PROJECTION>[];
  next?: string;
  consumedCapacity?: QueryCommandOutput['ConsumedCapacity'];
  count?: number;
  scannedCount?: number;
};

export interface QueryExecutor<TableType, PROJECTION> {
  input: QueryCommandInput;
  execute: () => Promise<QuerierReturn<TableType, PROJECTION>>;
}

export class DynamoQuerier<TableConfig extends TableDefinition> {
  constructor(
    private readonly tableConfig: TableConfig,
    private readonly clientConfig: DynamoConfig,
    private readonly parentKeys?: DynamoTableKeyConfig<TableConfig>,
  ) {}

  private keyExpression(
    keys: TableConfig['keys'],
    attributeBuilder: AttributeBuilder,
  ): string {
    const keyNames = Object.keys(keys);
    attributeBuilder.addNames(...keyNames);
    const hashValue = keys[this.tableConfig.keyNames.partitionKey];
    const valueKey = attributeBuilder.addValue(hashValue);
    const expression = `${attributeBuilder.nameFor(
      this.tableConfig.keyNames.partitionKey as string,
    )} = ${valueKey}`;
    if (
      this.tableConfig.keyNames.sortKey &&
      typeof keys[this.tableConfig.keyNames.sortKey] === 'function'
    ) {
      const keyOperation = new KeyOperation(
        this.tableConfig.keyNames.sortKey as string,
        new Wrapper(attributeBuilder),
      );
      (keys[this.tableConfig.keyNames.sortKey] as any)(keyOperation);
      return `${expression} AND ${keyOperation.wrapper.expression}`;
    }
    return expression;
  }

  query<const PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QuerierInput<TableConfig['type'], PROJECTION> = {},
  ): Promise<QuerierReturn<TableConfig['type'], PROJECTION>> {
    const executor = this.queryExecutor(keys, options);
    if (this.clientConfig.logStatements) {
      console.log(`QueryInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    return executor.execute();
  }

  async queryAll<const PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QueryAllInput<TableConfig['type'], PROJECTION> = {},
  ): Promise<QuerierReturn<TableConfig['type'], PROJECTION>> {
    const executor = this.queryAllExecutor(keys, options);
    if (this.clientConfig.logStatements) {
      console.log(`QueryAllInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    return executor.execute();
  }

  queryExecutor<const PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QuerierInput<TableConfig['type'], PROJECTION> = {},
  ): QueryExecutor<TableConfig['type'], PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyExpression(keys, attributeBuilder);
    const filterPart =
      options.filter && filterParts(attributeBuilder, options.filter);
    const projection =
      options.select && selectExpression(attributeBuilder, options.select);
    const input: QueryCommandInput = {
      TableName: this.clientConfig.tableName,
      ...(this.clientConfig.indexName
        ? { IndexName: this.clientConfig.indexName }
        : {}),
      ...{ KeyConditionExpression: keyExpression },
      ...(options.filter && filterPart ? { FilterExpression: filterPart } : {}),
      ...(options.select ? { ProjectionExpression: projection } : {}),
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
    const client = this.clientConfig.client;
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

  queryAllExecutor<const PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QueryAllInput<TableConfig['type'], PROJECTION> = {},
  ): QueryExecutor<TableConfig['type'], PROJECTION> {
    return new QueryAllExecutor(
      this.tableConfig,
      this.clientConfig,
      keys,
      options,
      (keys, builder) => this.keyExpression(keys, builder),
      this.parentKeys,
    );
  }
}

class QueryAllExecutor<TableConfig extends TableDefinition, PROJECTION>
  implements QueryExecutor<TableConfig, PROJECTION>
{
  input: QueryCommandInput;
  private readonly enrichedFields: string[];
  constructor(
    private readonly tableConfig: TableConfig,
    private readonly clientConfig: DynamoConfig,
    private readonly keys: KeyCompare<
      TableConfig['type'],
      TableConfig['keyNames']
    >,
    private readonly options: QueryAllInput<TableConfig['type'], PROJECTION>,
    private readonly createKeyExpression: (
      keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
      builder: AttributeBuilder,
    ) => string,
    readonly parentKeys?: DynamoTableKeyConfig<TableConfig['type']>,
    private readonly attributeBuilder = AttributeBuilder.create(),
  ) {
    // Paging needs the key attributes of the last item, so read them even if they weren't selected
    const keyFields = [
      this.tableConfig.keyNames.partitionKey,
      this.tableConfig.keyNames.sortKey,
      this.parentKeys?.partitionKey,
      this.parentKeys?.sortKey,
    ].filter((it): it is string => !!it);
    this.enrichedFields = this.options.select
      ? [...new Set(keyFields)].filter(
          (it) => !(this.options.select as readonly string[]).includes(it),
        )
      : [];
    const projection =
      this.options.select &&
      selectExpression(this.attributeBuilder, [
        ...this.options.select,
        ...this.enrichedFields,
      ]);
    this.input = {
      TableName: this.clientConfig.tableName,
      ...(this.clientConfig.indexName
        ? { IndexName: this.clientConfig.indexName }
        : {}),
      ...{
        KeyConditionExpression: this.createKeyExpression(
          this.keys,
          this.attributeBuilder,
        ),
      },
      ...(this.options.filter
        ? {
            FilterExpression: filterParts(
              this.attributeBuilder,
              this.options.filter,
            ),
          }
        : {}),
      ProjectionExpression: projection,
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

  private async queryPages(
    queryInput: QueryCommandInput,
    keyFields: {
      partitionKey: string;
      sortKey?: string;
      parentPartitionKey?: string;
      parentSortKey?: string;
    },
    enrichedFields?: string[],
    queryLimit?: number,
  ): Promise<{
    Items: QueryCommandOutput['Items'];
    LastEvaluatedKey?: string;
  }> {
    const limit = queryLimit ?? 0;
    const accumulation: Record<string, NativeAttributeValue>[] = [];
    let input = queryInput;
    let lastEvaluatedKey: any;
    for (;;) {
      const res = await this.clientConfig.client.query(input);
      const items = res.Items ?? [];
      if (limit > 0 && accumulation.length + items.length >= limit) {
        const remaining = limit - accumulation.length;
        accumulation.push(...items.slice(0, remaining));
        lastEvaluatedKey = this.buildNext(items[remaining - 1], keyFields);
        break;
      }
      accumulation.push(...items);
      if (!res.LastEvaluatedKey) break;
      input = { ...input, ExclusiveStartKey: res.LastEvaluatedKey };
    }
    if (enrichedFields) {
      this.removeFields(accumulation, enrichedFields);
    }
    return { Items: accumulation, LastEvaluatedKey: lastEvaluatedKey };
  }

  async execute(): Promise<QuerierReturn<TableConfig['type'], PROJECTION>> {
    const result = await this.queryPages(
      this.input,
      {
        partitionKey: this.tableConfig.keyNames.partitionKey as string,
        sortKey: this.tableConfig.keyNames.sortKey as string | undefined,
        parentPartitionKey: this.parentKeys?.partitionKey as string | undefined,
        parentSortKey: this.parentKeys?.sortKey as string | undefined,
      },
      this.enrichedFields,
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
