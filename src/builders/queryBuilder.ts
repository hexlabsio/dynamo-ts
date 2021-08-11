import { DocumentClient, IndexName } from 'aws-sdk/clients/dynamodb';
import { DDBClient } from 'src/client';
import {
  Conditions,
  KeyConditions,
  QueryOptions,
  QueryResult,
} from 'src/dynamoTypes';
import { ConditionBuilder } from './conditionBuilders';
import AttributeMap = DocumentClient.AttributeMap;

export interface QueryBuilder<T> {
  build(): {
    tableName: string;
    keyConditions: KeyConditions<T>;
    transform: (attributeMap: AttributeMap) => T;
  } & QueryOptions<T>;
}
export class MutableQueryBuilder<T> implements QueryBuilder<T> {
  private _filters?: Conditions<T, keyof T>;
  private _indexName?: IndexName;
  private _limit?: number;
  private _sort?: 'asc' | 'desc';
  private _projection?: Extract<keyof T, string>[];
  private _offsetKey?: Partial<T>;
  private _transform: (attributeMap: AttributeMap) => T = (attributeMap) =>
    attributeMap as T;
  constructor(
    private readonly ddbClient: DDBClient,
    private readonly tableName: string,
    private readonly keyConditions: KeyConditions<T>,
  ) {}
  filters(
    filters: Conditions<T, keyof T> | ConditionBuilder<T>,
  ): MutableQueryBuilder<T> {
    const builder = filters as ConditionBuilder<T>;
    if (builder.tag) {
      this._filters = builder.build();
    } else {
      this._filters = filters as Conditions<T, keyof T>;
    }
    return this;
  }
  index(indexName: string): MutableQueryBuilder<T> {
    this._indexName = indexName;
    return this;
  }
  limit(limit: number): MutableQueryBuilder<T> {
    this._limit = limit;
    return this;
  }
  sort(sort: 'asc' | 'desc'): MutableQueryBuilder<T> {
    this._sort = sort;
    return this;
  }
  offset(offsetKey: Partial<T>): MutableQueryBuilder<T> {
    this._offsetKey = offsetKey;
    return this;
  }
  transform(
    transform: (attributeMap: AttributeMap) => T,
  ): MutableQueryBuilder<T> {
    this._transform = transform;
    return this;
  }
  projection(
    ...projection: Extract<keyof T, string>[]
  ): MutableQueryBuilder<T> {
    this._projection = projection;
    return this;
  }
  build(): {
    tableName: string;
    keyConditions: KeyConditions<T>;
    transform: (attributeMap: AttributeMap) => T;
  } & QueryOptions<T> {
    return {
      tableName: this.tableName,
      keyConditions: this.keyConditions,
      transform: this._transform,
      filters: this._filters,
      index: this._indexName,
      limit: this._limit,
      sort: this._sort,
      projection: this._projection,
      offsetKey: this._offsetKey,
    };
  }

  async fetch(): Promise<QueryResult<T>> {
    return await this.ddbClient.query(
      this.tableName,
      this.keyConditions,
      {
        filters: this._filters,
        index: this._indexName,
        limit: this._limit,
        sort: this._sort,
        projection: this._projection,
        offsetKey: this._offsetKey,
      },
      this._transform,
    );
  }
}

export function queryFrom<T>(
  this: DDBClient,
  tableName: string,
  keyConditions: KeyConditions<T>,
): MutableQueryBuilder<T> {
  return new MutableQueryBuilder<T>(this, tableName, keyConditions);
}
