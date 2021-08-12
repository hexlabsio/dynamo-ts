import { DynamoDB } from 'aws-sdk';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import * as crypto from 'crypto';
import GetItemInput = DocumentClient.GetItemInput;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import QueryInput = DocumentClient.QueryInput;

type SimpleDynamoType =
  | 'string'
  | 'string set'
  | 'number'
  | 'number set'
  | 'binary set'
  | 'binary'
  | 'boolean'
  | 'null'
  | 'list'
  | 'map';

type KeyComparisonBuilder<T> = {
  eq(value: T): void;
  lt(value: T): void;
  lte(value: T): void;
  gt(value: T): void;
  gte(value: T): void;
  between(a: T, b: T): void;
} & (T extends string ? { beginsWith(value: string): void } : {});

type ComparisonBuilder<T> = { [K in keyof T]: Operation<T, T[K]> } & {
  exists(path: string): CompareWrapperOperator<T>;
  notExists(path: string): CompareWrapperOperator<T>;
  isType(path: string, type: SimpleDynamoType): CompareWrapperOperator<T>;
  beginsWith(path: string, beginsWith: string): CompareWrapperOperator<T>;
  contains(path: string, operand: string): CompareWrapperOperator<T>;
  not(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
};

type CompareWrapperOperator<T> = {
  and(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
  or(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
};

type Operation<T, V> = {
  eq(value: V): CompareWrapperOperator<T>;
  neq(value: V): CompareWrapperOperator<T>;
  lt(value: V): CompareWrapperOperator<T>;
  lte(value: V): CompareWrapperOperator<T>;
  gt(value: V): CompareWrapperOperator<T>;
  gte(value: V): CompareWrapperOperator<T>;
  between(a: V, b: V): CompareWrapperOperator<T>;
  in(a: V, b: V[]): CompareWrapperOperator<T>;
};

export type DynamoType = SimpleDynamoType | DynamoEntryDefinition;
export type DynamoEntryDefinition = { [key: string]: DynamoType };

export type TypeFor<T extends DynamoType> = T extends 'string'
  ? string
  : T extends 'string set'
  ? string[]
  : T extends 'number'
  ? number
  : T extends 'number set'
  ? number[]
  : T extends 'number'
  ? Buffer
  : T extends 'number set'
  ? Buffer[]
  : T extends 'list'
  ? unknown[]
  : T extends 'map'
  ? Record<string, unknown>
  : T extends 'boolean'
  ? boolean
  : T extends 'null'
  ? null
  : T extends DynamoEntryDefinition
  ? { [K in keyof T]: DynamoEntry<T>[K] }
  : never;

export type DynamoEntry<T extends DynamoEntryDefinition> = {
  [K in keyof T]: TypeFor<T[K]>;
};

class KeyOperation<T> {
  public wrapper = new Wrapper();
  constructor(private readonly key: string) {}

  private add(expression: (key: string) => string): (value: T) => void {
    return (value) => {
      const mappedKey = nameFor(this.key);
      return this.wrapper.add(
        { [`#${mappedKey}`]: this.key },
        { [`:${mappedKey}`]: value },
        expression(mappedKey),
      ) as any;
    };
  }

  eq = this.add((key) => `#${key} = :${key}`);
  neq = this.add((key) => `#${key} <> :${key}`);
  lt = this.add((key) => `#${key} < :${key}`);
  lte = this.add((key) => `#${key} <= :${key}`);
  gt = this.add((key) => `#${key} > :${key}`);
  gte = this.add((key) => `#${key} >= :${key}`);
}

class OperationType {
  constructor(
    private readonly wrapper: Wrapper,
    private readonly key: string,
  ) {}
  operation(): Operation<any, any> {
    return this as unknown as Operation<any, any>;
  }

  private add(
    expression: (key: string) => string,
  ): (value: TypeFor<DynamoType>) => CompareWrapperOperator<any> {
    return (value) => {
      const mappedKey = nameFor(this.key);
      return this.wrapper.add(
        { [`#${mappedKey}`]: this.key },
        { [`:${mappedKey}`]: value },
        expression(mappedKey),
      ) as any;
    };
  }

  eq = this.add((key) => `#${key} = :${key}`);
  neq = this.add((key) => `#${key} <> :${key}`);
  lt = this.add((key) => `#${key} < :${key}`);
  lte = this.add((key) => `#${key} <= :${key}`);
  gt = this.add((key) => `#${key} > :${key}`);
  gte = this.add((key) => `#${key} >= :${key}`);

  between(
    a: TypeFor<DynamoType>,
    b: TypeFor<DynamoType>,
  ): CompareWrapperOperator<any> {
    const mappedKey = nameFor(this.key);
    const aKey = `:${mappedKey}1`;
    const bKey = `:${mappedKey}2`;
    return this.wrapper.add(
      { [`#${mappedKey}`]: this.key },
      { [aKey]: a, [bKey]: b },
      `#${mappedKey} BETWEEN ${aKey} AND ${bKey}`,
    ) as any;
  }
  in(list: TypeFor<DynamoType>[]): CompareWrapperOperator<any> {
    const mappedKey = nameFor(this.key);
    const valueMappings = list.reduce(
      (agg, it, index) => ({ ...agg, [`:${mappedKey}${index}`]: it }),
      {} as any,
    );
    return this.wrapper.add(
      { [`#${mappedKey}`]: this.key },
      valueMappings,
      `#${mappedKey} IN (${Object.keys(valueMappings)
        .map((it) => `:${it}`)
        .join(',')})`,
    ) as any;
  }
}

class ComparisonBuilderType<
  D extends DynamoEntryDefinition,
  T extends DynamoEntry<D>,
> {
  public wrapper = new Wrapper();
  constructor(definition: D) {
    Object.keys(definition).forEach((key) => {
      (this as any)[key] = new OperationType(this.wrapper, key).operation();
    });
  }

  exists(path: string): Wrapper {
    return this.wrapper.add({}, {}, `attribute_exists(${path})`);
  }
  notExists(path: string): Wrapper {
    return this.wrapper.add({}, {}, `attribute_not_exists(${path})`);
  }
  private typeFor(type: SimpleDynamoType): string {
    switch (type) {
      case 'string':
        return 'S';
      case 'string set':
        return 'SS';
      case 'number':
        return 'N';
      case 'number set':
        return 'NS';
      case 'binary':
        return 'B';
      case 'binary set':
        return 'BS';
      case 'boolean':
        return 'BOOL';
      case 'null':
        return 'NULL';
      case 'list':
        return 'L';
      case 'map':
        return 'M';
    }
  }

  isType(path: string, type: SimpleDynamoType): Wrapper {
    const key = Math.floor(Math.random() * 10000000);
    return this.wrapper.add(
      {},
      { [key]: this.typeFor(type) },
      `attribute_type(${path}, :${key})`,
    );
  }

  beginsWith(path: string, beginsWith: string): Wrapper {
    const key = Math.floor(Math.random() * 10000000);
    return this.wrapper.add(
      {},
      { [key]: beginsWith },
      `begins_with(${path}, :${key})`,
    );
  }

  contains(path: string, operand: string): Wrapper {
    const key = Math.floor(Math.random() * 10000000);
    return this.wrapper.add(
      {},
      { [key]: operand },
      `operand(${path}, :${key})`,
    );
  }

  not(comparison: Wrapper): Wrapper {
    this.wrapper.names = comparison.names;
    this.wrapper.valueMappings = comparison.valueMappings;
    this.wrapper.expression = `NOT (${comparison.expression})`;
    return this.wrapper;
  }

  builder(): ComparisonBuilder<T> {
    return this as unknown as ComparisonBuilder<T>;
  }
}

class Wrapper {
  constructor(
    public names: Record<string, string> = {},
    public valueMappings: Record<string, unknown> = {},
    public expression: string = '',
  ) {}

  add(
    names: Record<string, string> = {},
    valueMappings: Record<string, unknown> = {},
    expression: string = '',
  ): Wrapper {
    this.names = { ...this.names, ...names };
    this.valueMappings = { ...this.valueMappings, ...valueMappings };
    this.expression = expression;
    return this;
  }

  and(comparison: Wrapper): Wrapper {
    this.add(
      comparison.names,
      comparison.valueMappings,
      `(${this.expression}) AND (${comparison.expression})`,
    );
    return this;
  }

  or(comparison: Wrapper): Wrapper {
    this.add(
      comparison.names,
      comparison.valueMappings,
      `(${this.expression}) OR (${comparison.expression})`,
    );
    return this;
  }
}

export interface TableDefinition<
  D extends DynamoEntryDefinition,
  T extends DynamoEntry<D>,
  H extends keyof D,
  R extends keyof D | null = null,
  G extends Record<
    string,
    { hashKey: keyof D; rangeKey?: keyof D }
  > | null = null,
> {}

export interface DynamoTableIndex<
  T,
  H extends keyof T,
  R extends keyof T | null = null,
> {
  query<P extends (keyof T)[] | null = null>(
    queryParameters: { [K in H]: T[K] } &
      (R extends string
        ? { [K in R]?: (sortKey: KeyComparisonBuilder<T[R]>) => any }
        : {}) & {
        filter?: (
          compare: () => ComparisonBuilder<
            Omit<T, R extends string ? H | R : H>
          >,
        ) => CompareWrapperOperator<Omit<T, R extends string ? H | R : H>>;
      } & { projection?: P; next?: string } & {
        dynamo?: Omit<
          QueryInput,
          | 'TableName'
          | 'IndexName'
          | 'KeyConditionExpression'
          | 'ProjectionExpression'
          | 'FilterExpression'
          | 'ExclusiveStartKey'
        >;
      },
  ): Promise<{
    next?: string;
    member: P extends (keyof T)[]
      ? { [K in R extends string ? P[number] | H | R : P[number] | H]: T[K] }[]
      : { [K in keyof T]: T[K] }[];
  }>;
}

export class DynamoTable<
  D extends DynamoEntryDefinition,
  H extends keyof D,
  R extends keyof D | null = null,
  G extends Record<
    string,
    { hashKey: keyof D; rangeKey?: keyof D }
  > | null = null,
> implements TableDefinition<D, DynamoEntry<D>, H, R, G>
{
  public readonly tableEntry: { [K in keyof D]: DynamoEntry<D>[K] } =
    undefined as any;

  protected constructor(
    protected readonly table: string,
    protected readonly dynamo: DynamoDB.DocumentClient,
    private readonly definition: D,
    private readonly hashKey: H,
    private readonly rangeKey?: R,
    private readonly indexes?: G,
    protected readonly indexName?: string,
  ) {}

  async get(
    key: { [K in R extends string ? H | R : H]: DynamoEntry<D>[K] },
    extras: Omit<GetItemInput, 'TableName' | 'Key'> = {},
  ): Promise<DynamoEntry<D> | undefined> {
    const result = await this.dynamo
      .get({
        TableName: this.table,
        Key: key,
        ProjectionExpression: Object.keys(this.definition).join(','),
        ...extras,
      })
      .promise();
    return result.Item as DynamoEntry<D> | undefined;
  }

  async put(item: DynamoEntry<D>): Promise<DynamoEntry<D>> {
    await this.dynamo.put({ TableName: this.table, Item: item }).promise();
    return item;
  }

  async delete(
    key: { [K in R extends string ? H | R : H]: DynamoEntry<D>[K] },
  ): Promise<void> {
    await this.dynamo.delete({ TableName: this.table, Key: key }).promise();
  }

  async update(
    key: { [K in R extends string ? H | R : H]: DynamoEntry<D>[K] },
    updates: Partial<Omit<DynamoEntry<D>, R extends string ? H | R : H>>,
    increment?: keyof Omit<DynamoEntry<D>, R extends string ? H | R : H>,
    start?: unknown,
    extras?: Partial<UpdateItemInput>,
  ): Promise<DynamoEntry<D> | undefined> {
    const result = await this.dynamo
      .update({
        TableName: this.table,
        Key: key,
        ...this.updateExpression(updates, increment, start),
        ...(extras ?? {}),
      })
      .promise();
    return result.Attributes as DynamoEntry<D> | undefined;
  }

  async scan(
    next?: string,
  ): Promise<{ member: DynamoEntry<D>[]; next?: string }> {
    const result = await this.dynamo
      .scan({
        TableName: this.table,
        ProjectionExpression: Object.keys(this.definition).join(','),
        ...(next
          ? {
              ExclusiveStartKey: JSON.parse(
                Buffer.from(next, 'base64').toString('ascii'),
              ),
            }
          : {}),
      })
      .promise();
    return {
      member: (result.Items ?? []) as DynamoEntry<D>[],
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
            'base64',
          )
        : undefined,
    };
  }

  index<K extends keyof G>(
    index: K,
  ): G extends {}
    ? DynamoTableIndex<
        DynamoEntry<D>,
        G[K]['hashKey'],
        G[K]['rangeKey'] extends string ? G[K]['rangeKey'] : null
      >
    : never {
    const indexDef = this.indexes![index];
    return new DynamoTable(
      this.table,
      this.dynamo,
      this.definition,
      indexDef.hashKey,
      indexDef.rangeKey,
      undefined,
      index as string,
    ) as any;
  }

  async query<P extends (keyof DynamoEntry<D>)[] | null = null>(
    queryParameters: { [K in H]: DynamoEntry<D>[K] } &
      (R extends string
        ? {
            [K in R]?: (
              sortKey: KeyComparisonBuilder<DynamoEntry<D>[R]>,
            ) => any;
          }
        : {}) & {
        filter?: (
          compare: () => ComparisonBuilder<
            Omit<DynamoEntry<D>, R extends string ? H | R : H>
          >,
        ) => CompareWrapperOperator<
          Omit<DynamoEntry<D>, R extends string ? H | R : H>
        >;
      } & { projection?: P; next?: string } & {
        dynamo?: Omit<
          QueryInput,
          | 'TableName'
          | 'IndexName'
          | 'KeyConditionExpression'
          | 'ProjectionExpression'
          | 'FilterExpression'
          | 'ExclusiveStartKey'
        >;
      },
  ): Promise<{
    next?: string;
    member: P extends (keyof DynamoEntry<D>)[]
      ? {
          [K in R extends string
            ? P[number] | H | R
            : P[number] | H]: DynamoEntry<D>[K];
        }[]
      : { [K in keyof DynamoEntry<D>]: DynamoEntry<D>[K] }[];
  }> {
    const keyPart = this.keyPart(queryParameters);
    const filterPart = this.filterPart(queryParameters);
    const actualProjection: string[] = (queryParameters.projection ??
      Object.keys(this.definition)) as string[];
    const projectionNameMappings = actualProjection.reduce(
      (acc, it) => ({ ...acc, [`#${nameFor(it as string)}`]: it as string }),
      {},
    );
    const queryInput: QueryInput = {
      TableName: this.table,
      ...(this.indexName ? { IndexName: this.indexName } : {}),
      ...keyPart,
      ...(filterPart.FilterExpression
        ? { FilterExpression: filterPart.FilterExpression }
        : {}),
      ExpressionAttributeNames: {
        ...keyPart.ExpressionAttributeNames,
        ...filterPart.ExpressionAttributeNames,
        ...projectionNameMappings,
      },
      ExpressionAttributeValues: {
        ...keyPart.ExpressionAttributeValues,
        ...filterPart.ExpressionAttributeValues,
      },
      ...(actualProjection
        ? {
            ProjectionExpression: Object.keys(projectionNameMappings).join(','),
          }
        : {}),
      ...(queryParameters.dynamo ?? {}),
      ...(queryParameters.next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(queryParameters.next, 'base64').toString('ascii'),
            ),
          }
        : {}),
    };
    const result = await this.dynamo.query(queryInput).promise();
    return {
      member: (result.Items ?? []) as DynamoEntry<D>[],
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
            'base64',
          )
        : undefined,
    };
  }

  private keyPart(
    query: { [K in H]: DynamoEntry<D>[K] } &
      (R extends string
        ? {
            [K in R]?: (
              sortKey: KeyComparisonBuilder<DynamoEntry<D>[R]>,
            ) => any;
          }
        : {}),
  ): Pick<
    QueryInput,
    | 'KeyConditionExpression'
    | 'ExpressionAttributeNames'
    | 'ExpressionAttributeValues'
  > {
    const hashValue = query[this.hashKey];
    const expression = '#hash = :hash';
    const names = { ['#hash']: this.hashKey as string };
    const values = { [':hash']: hashValue };
    if (this.rangeKey && (query as any)[this.rangeKey]) {
      const keyOperation = new KeyOperation(this.rangeKey as string);
      (query as any)[this.rangeKey](keyOperation);
      return {
        KeyConditionExpression: `${expression} AND ${keyOperation.wrapper.expression}`,
        ExpressionAttributeNames: { ...names, ...keyOperation.wrapper.names },
        ExpressionAttributeValues: {
          ...values,
          ...keyOperation.wrapper.valueMappings,
        },
      };
    }
    return {
      KeyConditionExpression: expression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
    };
  }

  private filterPart(query: {
    filter?: (
      compare: () => ComparisonBuilder<
        Omit<DynamoEntry<D>, R extends string ? H | R : H>
      >,
    ) => CompareWrapperOperator<
      Omit<DynamoEntry<D>, R extends string ? H | R : H>
    >;
  }): Pick<
    QueryInput,
    | 'FilterExpression'
    | 'ExpressionAttributeNames'
    | 'ExpressionAttributeValues'
  > {
    if (query.filter) {
      const updatedDefinition = Object.keys(this.definition)
        .filter((it) => it !== this.hashKey && it !== this.rangeKey)
        .reduce((acc, it) => ({ ...acc, [it]: this.definition[it] }), {});
      const builder = () =>
        new ComparisonBuilderType(updatedDefinition).builder();
      const parent = query.filter(builder as any) as unknown as Wrapper;
      return {
        FilterExpression: parent.expression,
        ExpressionAttributeNames: parent.names,
        ExpressionAttributeValues: parent.valueMappings,
      };
    }
    return { ExpressionAttributeValues: {}, ExpressionAttributeNames: {} };
  }

  private updateExpression(
    properties: Partial<Omit<DynamoEntry<D>, R extends string ? H | R : H>>,
    increment?: keyof Omit<DynamoEntry<D>, R extends string ? H | R : H>,
    start?: unknown,
  ): {
    UpdateExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues: Record<string, any>;
  } {
    const props = properties as any;
    const validKeys = Object.keys(properties).filter((it) => !!props[it]);
    const removes = Object.keys(properties).filter((it) => !props[it]);
    const updateExpression =
      `SET ${validKeys
        .map(
          (key) =>
            `#${nameFor(key)} = ${
              increment === key
                ? `${
                    start !== undefined
                      ? `if_not_exists(${key}, :start)`
                      : `#${nameFor(key)}`
                  } + `
                : ''
            }:${nameFor(key)}`,
        )
        .join(', ')}` +
      (removes.length > 0
        ? ` REMOVE ${removes.map((key) => `#${nameFor(key)}`).join(', ')}`
        : '');
    const names = [...validKeys, ...removes].reduce(
      (names, key) => ({ ...names, [`#${nameFor(key)}`]: key }),
      {},
    );
    const values = validKeys.reduce(
      (values, key) => ({ ...values, [`:${nameFor(key)}`]: props[key] }),
      {},
    );
    return {
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues:
        start !== undefined ? { ...values, [':start']: start } : values,
    };
  }

  static build<
    D extends DynamoEntryDefinition,
    H extends keyof D,
    R extends keyof D | null = null,
    G extends Record<
      string,
      { hashKey: keyof D; rangeKey?: keyof D }
    > | null = null,
  >(
    table: string,
    dynamo: DynamoDB.DocumentClient,
    definition: TableEntryDefinition<D, H, R, G>,
  ): DynamoTable<D, H, R, G> {
    return new DynamoTable<D, H, R, G>(
      table,
      dynamo,
      definition.definition,
      definition.hashKey,
      definition.rangeKey,
      definition.indexes,
      undefined,
    );
  }
}

function nameFor(name: string): string {
  return crypto.createHash('md5').update(name).digest('hex');
}

export type TableEntryDefinition<
  D extends DynamoEntryDefinition,
  H extends keyof D,
  R extends keyof D | null = null,
  G extends Record<
    string,
    { hashKey: keyof D; rangeKey?: keyof D }
  > | null = null,
> = { definition: D; hashKey: H; rangeKey?: R; indexes?: G };
