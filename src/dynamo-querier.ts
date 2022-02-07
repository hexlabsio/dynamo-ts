import {
  DynamoEntry,
  DynamoIndexes,
  DynamoMapDefinition,
} from './type-mapping';
import { DynamoClientConfig, DynamoDefinition } from './dynamo-client-config';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import QueryInput = DocumentClient.QueryInput;
import { filterParts, KeyComparisonBuilder, Wrapper } from './comparison';
import { AttributeBuilder } from './attribute-builder';
import { KeyOperation } from './operation';
import { DynamoFilter } from './filter';
import { Projection, ProjectionHandler } from './projector';
import { AttributeMap } from 'aws-sdk/clients/dynamodb';

type HashComparison<HASH extends keyof T, T> = {
  [K in HASH]: T[K];
};

type RangeComparison<R extends keyof T, T> = {
  [K in R]?: (sortKey: KeyComparisonBuilder<T[R]>) => any;
};

type RangeComparisonIfExists<R extends keyof T | null, T> = R extends string
  ? RangeComparison<R, T>
  : {};

type Filter<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
> = {
  filter?: DynamoFilter<DEFINITION, HASH, RANGE>;
};

type ExcessParameters = Omit<
  QueryInput,
  | 'TableName'
  | 'IndexName'
  | 'KeyConditionExpression'
  | 'FilterExpression'
  | 'ExclusiveStartKey'
>;

export type QueryParametersInput<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
  PROJECTED = null,
> = HashComparison<HASH, DynamoEntry<DEFINITION>> &
  RangeComparisonIfExists<RANGE, DynamoEntry<DEFINITION>> &
  Filter<DEFINITION, HASH, RANGE> & {
    projection?: Projection<DEFINITION, PROJECTED>;
    next?: string;
    dynamo?: ExcessParameters;
  };

export type QueryAllParametersInput<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
  PROJECTED = null,
> = QueryParametersInput<DEFINITION, HASH, RANGE, PROJECTED> & {
  queryLimit?: number;
};

export class DynamoQuerier {
  private static keyPart<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
  >(
    definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
    attributeBuilder: AttributeBuilder,
    queryParameters: HashComparison<HASH, DynamoEntry<DEFINITION>> &
      RangeComparisonIfExists<RANGE, DynamoEntry<DEFINITION>>,
  ): string {
    attributeBuilder.addNames(definition.hash as string);
    const hashValue = queryParameters[definition.hash];
    const valueKey = attributeBuilder.addValue(hashValue);
    const expression = `${attributeBuilder.nameFor(
      definition.hash as string,
    )} = ${valueKey}`;
    if (definition.range && (queryParameters as any)[definition.range]) {
      const keyOperation = new KeyOperation(
        definition.range as string,
        new Wrapper(attributeBuilder),
      );
      (queryParameters as any)[definition.range](keyOperation);
      return `${expression} AND ${keyOperation.wrapper.expression}`;
    }
    return expression;
  }

  static async query<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
    PROJECTED = null,
  >(
    config: DynamoClientConfig<DEFINITION>,
    definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
    options: QueryParametersInput<DEFINITION, HASH, RANGE, PROJECTED>,
  ): Promise<{
    next?: string;
    member: (PROJECTED extends null
      ? {
          [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K];
        }
      : PROJECTED)[];
  }> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyPart(definition, attributeBuilder, options);
    const filterPart =
      options.filter &&
      filterParts(definition, attributeBuilder, options.filter);
    const projection = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      config.definition,
      options.projection,
    );
    const queryInput = {
      TableName: config.tableName,
      ...(config.indexName ? { IndexName: config.indexName } : {}),
      ...{ KeyConditionExpression: keyExpression },
      ...(options.filter ? { FilterExpression: filterPart } : {}),
      ProjectionExpression: projection,
      ...attributeBuilder.asInput(options.dynamo),
      ...(options.next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(options.next, 'base64').toString('ascii'),
            ),
          }
        : {}),
    };
    if (config.logStatements) {
      console.log(`QueryInput: ${JSON.stringify(queryInput, null, 2)}`);
    }

    const result = await config.client.query(queryInput).promise();
    return {
      member: (result.Items ?? []) as any[],
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
            'base64',
          )
        : undefined,
    } as any;
  }

  static async queryAll<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
    PROJECTED = null,
  >(
    config: DynamoClientConfig<DEFINITION>,
    definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
    options: QueryAllParametersInput<DEFINITION, HASH, RANGE, PROJECTED>,
  ): Promise<{
    next?: string;
    member: (PROJECTED extends null
      ? {
          [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K];
        }
      : PROJECTED)[];
  }> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyPart(definition, attributeBuilder, options);
    const filterPart =
      options.filter &&
      filterParts(definition, attributeBuilder, options.filter);

    const indexName =
      config.indexName && definition.indexes
        ? definition.indexes[config.indexName]?.rangeKey ?? null
        : null;
    const [projection, enrichedFields] =
      ProjectionHandler.projectionWithKeysFor(
        attributeBuilder,
        config.definition,
        definition.hash,
        definition.range,
        indexName,
        options.projection,
      );
    const queryInput = {
      TableName: config.tableName,
      KeyConditionExpression: keyExpression,
      ...(config.indexName ? { IndexName: config.indexName } : {}),
      ...(options.filter ? { FilterExpression: filterPart } : {}),
      ProjectionExpression: projection,
      ...attributeBuilder.asInput(options.dynamo),
      ...(options.next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(options.next, 'base64').toString('ascii'),
            ),
          }
        : {}),
      ...(options.dynamo ? options.dynamo : {})  
    };
    if (config.logStatements) {
      console.log(`QueryInput: ${JSON.stringify(queryInput, null, 2)}`);
    }
    definition.hash as string;
    const result = await this._recQuery(
      config.client,
      queryInput,
      {
        hashKey: definition.hash as string,
        rangeKey: definition.range ? (definition.range as string) : undefined,
        indexKey: indexName ? (indexName as string) : undefined,
      },
      enrichedFields,
      options.queryLimit,
    );
    return {
      member: (result.Items ?? []) as any[],
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
            'base64',
          )
        : undefined,
    } as any;
  }

  private static buildNext(
    lastItem: AttributeMap,
    keyFields: { hashKey: string; rangeKey?: string; indexKey?: string },
  ): string {
    const nextKey = { [keyFields.hashKey]: lastItem[keyFields.hashKey] };
    if (keyFields.rangeKey)
      nextKey[keyFields.rangeKey] = lastItem[keyFields.rangeKey];
    if (keyFields.indexKey)
      nextKey[keyFields.indexKey] = lastItem[keyFields.indexKey];
    return Buffer.from(JSON.stringify(nextKey)).toString('base64');
  }

  private static removeFields(
    lastItems: AttributeMap[],
    enrichedFields: string[],
  ): void {
    lastItems.forEach((lastItem) =>
      enrichedFields.forEach((field) => delete lastItem[field]),
    );
  }
  
  private static async _recQuery(
    client: DocumentClient,
    queryInput: QueryInput,
    keyFields: { hashKey: string; rangeKey?: string; indexKey?: string },
    enrichedFields?: string[],
    queryLimit?: number,
    accumulation: AttributeMap[] = [],
    accumulationCount?: number,
  ): Promise<{ Items: AttributeMap[]; LastEvaluatedKey?: string }> {
    const res = await client.query(queryInput).promise();

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
    } else {
      return await this._recQuery(
        client,
        { ...queryInput, ExclusiveStartKey: res.LastEvaluatedKey },
        keyFields,
        enrichedFields,
        queryLimit,
        [...accumulation, ...(res.Items ?? [])],
        updatedAccLength,
      );
    }
  }
}
