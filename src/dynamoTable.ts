import { DynamoDB } from 'aws-sdk';
import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import * as crypto from 'crypto';
import GetItemInput = DocumentClient.GetItemInput;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import QueryInput = DocumentClient.QueryInput;
import WriteRequests = DocumentClient.WriteRequests;

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
  | 'map'
  | 'string?'
  | 'string set?'
  | 'number?'
  | 'number set?'
  | 'binary set?'
  | 'binary?'
  | 'boolean?'
  | 'list?'
  | 'map?';

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
export type DynamoObjectDefinition = {optional?: boolean, object: { [key: string]: DynamoType } };
export type DynamoArrayDefinition = {optional?: boolean, array: DynamoType };
export type DynamoEntryDefinition = DynamoObjectDefinition | DynamoArrayDefinition;

export type TypeFor<T extends DynamoType> = T extends 'string'
  ? string
  : T extends 'string set'
  ? string[]
  : T extends 'number'
  ? number
  : T extends 'number set'
  ? number[]
  : T extends 'binary'
  ? Buffer
  : T extends 'binary set'
  ? Buffer[]
  : T extends 'list'
  ? unknown[]
  : T extends 'map'
  ? Record<string, unknown>
  : T extends 'boolean'
  ? boolean
  : T extends 'null'
  ? null :
  T extends 'string?'
  ? string | undefined
  : T extends 'string set?'
  ? string[] | undefined
  : T extends 'number?'
  ? number | undefined
  : T extends 'number set?'
  ? number[] | undefined
  : T extends 'binary?'
  ? Buffer | undefined
  : T extends 'binary set?'
  ? Buffer[] | undefined
  : T extends 'list?'
  ? unknown[] | undefined
  : T extends 'map?'
  ? Record<string, unknown>
  : T extends 'boolean'
  ? boolean
  : T extends DynamoEntryDefinition
  ? (T['optional'] extends true ? (T extends {object: any} ? { [K in keyof T['object']]: DynamoAnyEntry<T['object']>[K] } : (T extends {array: any} ? DynamoAnyEntry<T['array']> : never)) | undefined : (T extends {object: any} ? { [K in keyof T['object']]: DynamoAnyEntry<T['object']>[K] } : (T extends {array: any} ? DynamoAnyEntry<T['array']> : never)))
  : never;

type UndefinedKeys<T> = { [P in keyof T]: undefined extends T[P] ? P : never}[keyof T];
type PartializeTop<T> = Partial<Pick<T, UndefinedKeys<T>>> & Omit<T, UndefinedKeys<T>>;
type PartializeObj<T> = {[K in keyof T]: T[K] extends Record<string, unknown> ? Partialize<T[K]>: T[K] extends (infer A)[] ? (A extends Record<string, unknown> ? Partialize<A> : A)[]: T[K]};
type Partialize<T> = PartializeObj<PartializeTop<T>>

export type DynamoEntry<T extends DynamoObjectDefinition['object']> = Partialize<{
  [K in keyof T]: TypeFor<T[K]>;
}>
export type DynamoAnyEntry<T extends DynamoArrayDefinition['array'] | DynamoObjectDefinition['object']> = T extends DynamoObjectDefinition['object'] ? {
  [K in keyof T]: TypeFor<T[K]>;
} : T extends DynamoArrayDefinition['array'] ? TypeFor<T>[] : never;

class KeyOperation<T> {
  public wrapper = new Wrapper();
  constructor(private readonly key: string) {}

  private add(expression: (key: string) => string): (value: T) => Wrapper {
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
  
  between(a: T, b: T): Wrapper {
    const mappedKey = nameFor(this.key);
    return this.wrapper.add(
      { [`#${mappedKey}`]: this.key },
      { [`:${mappedKey}1`]: a, [`:${mappedKey}2`]: b },
      `#${mappedKey} BETWEEN :${mappedKey}1 AND :${mappedKey}2`
    );
  }
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
  D extends DynamoObjectDefinition['object'],
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
    const withoutOptional = type.endsWith('?') ? type.substring(0, type.length - 2) : type;
    switch (withoutOptional) {
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
      default:
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
  D extends DynamoObjectDefinition['object'],
  T extends DynamoEntry<D>,
  H extends keyof T,
  R extends keyof T | null = null,
  G extends Record<
    string,
    { hashKey: keyof T; rangeKey?: keyof T }
  > | null = null,
> 
{
  public readonly tableEntry: T =
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
    key: { [K in R extends string ? H | R : H]: T[K] },
    extras: Omit<GetItemInput, 'TableName' | 'Key'> = {}, 
    logStatement?: boolean
  ): Promise<T | undefined> {
    const actualProjection =  Object.keys(this.definition) as string[];
    const projectionNameMappings = actualProjection.reduce(
      (acc, it) => ({ ...acc, [`#${nameFor(it as string)}`]: it as string }),
      {},
    );
    const getInput = {
      TableName: this.table,
      Key: key,
      ProjectionExpression: extras.ProjectionExpression ?? Object.keys(projectionNameMappings).join(','),
      ...extras,
      ExpressionAttributeNames: {...(extras.ExpressionAttributeNames ?? {}), ...projectionNameMappings}
    }
    if(logStatement) {
      console.log(`getInput: ${JSON.stringify(getInput, null, 2)}`)
    }
    const result = await this.dynamo
      .get(getInput)
      .promise();
    return result.Item as T | undefined;
  }
  
  async batchGet<P extends (keyof T)[] | null = null>(keys: { [K in R extends string ? H | R : H]: T[K] }[], projection?: P, consistent?: boolean, logStatement?: boolean)
  : Promise<P extends (keyof T)[]
    ? { [K in R extends string ? P[number] | H | R : P[number] | H]: T[K] }[]
    : { [K in keyof T]: T[K] }[]>{
    const actualProjection =  (projection ?? Object.keys(this.definition)) as string[];
    const projectionNameMappings = actualProjection.reduce(
      (acc, it) => ({ ...acc, [`#${nameFor(it as string)}`]: it as string }),
      {},
    );
    const batchGetInput = {RequestItems: {[this.table]: {Keys: keys, ...(projection ? {ProjectionExpression: Object.keys(projectionNameMappings).join(',')} : {}), ...((consistent !== undefined) ? {ConsistentRead: consistent}: {})}}}
    if(logStatement) {
      console.log(`batchGetInput: ${JSON.stringify(batchGetInput, null, 2)}`)
    }
    const result = await this.dynamo.batchGet(batchGetInput).promise();
    return result.Responses![this.table] as any;
  }
  
  async directBatchWrite(writeRequest: WriteRequests, opType: string, logStatement?: boolean): Promise<{ unprocessed?: WriteRequests }> {
    const batchWriteInput = {RequestItems: {[this.table]: writeRequest}}
    if(logStatement) {
      console.log(`${opType}: ${JSON.stringify(batchWriteInput, null, 2)}`)
    }
    const result = await this.dynamo.batchWrite(batchWriteInput).promise();
    return { unprocessed: result.UnprocessedItems?.[this.table] };
  }
  
  batchWrite(operations: ({delete: { [K in R extends string ? H | R : H]: T[K] }} | {put: T})[], opType: string, logStatement?: boolean): Promise<{ unprocessed?: WriteRequests }> {
    return this.directBatchWrite(operations.map(operation => {
          return (operation as any).put ? {PutRequest: { Item: (operation as any).put }} : {Delete: { Key: (operation as any).delete }};
    }), opType, logStatement);
  }
  
  async batchPut(operations: T[], logStatement?: boolean): Promise<void> {
    await this.batchWrite(operations.map(it =>({put: it})), "batchPut", logStatement)
  }
  
  async batchDelete(operations: { [K in R extends string ? H | R : H]: T[K] }[], logStatement?: boolean): Promise<void> {
    await this.batchWrite(operations.map(it =>({delete: it})), "batchDelete", logStatement)
  }

  async put(item: T, logStatement?: boolean): Promise<DynamoEntry<D>> {
    const putInput = { TableName: this.table, Item: item }
    if(logStatement) {
      console.log(`putInput: ${JSON.stringify(putInput, null, 2)}`)
    }
    await this.dynamo.put(putInput).promise();
    return item;
  }

  async delete(
    key: { [K in R extends string ? H | R : H]: T[K] },
    logStatement?: boolean
  ): Promise<void> {
    const deleteInput = { TableName: this.table, Key: key }
    if(logStatement) {
      console.log(`deleteInput: ${JSON.stringify(deleteInput, null, 2)}`)
    }
    await this.dynamo.delete(deleteInput).promise();
  }

  async update(
    key: { [K in R extends string ? H | R : H]: T[K] },
    updates: Partial<Omit<T, R extends string ? H | R : H>>,
    increment?: keyof Omit<T, R extends string ? H | R : H>,
    start?: unknown,
    extras?: Partial<UpdateItemInput>,
    logStatement?: boolean
  ): Promise<T | undefined> {
    const updateInput = {
      TableName: this.table,
      Key: key,
      ...this.updateExpression(updates, increment, start),
      ...(extras ?? {}),
    }
    if(logStatement) {
      console.log(`updateInput: ${JSON.stringify(updateInput, null, 2)}`)
    }
    const result = await this.dynamo
      .update(updateInput)
      .promise();
    return result.Attributes as T | undefined;
  }

  async scan(
    next?: string,
    logStatement?: boolean
  ): Promise<{ member: T[]; next?: string }> {
    const actualProjection =  Object.keys(this.definition) as string[];
    const projectionNameMappings = actualProjection.reduce(
      (acc, it) => ({ ...acc, [`#${nameFor(it as string)}`]: it as string }),
      {},
    );
    const scanInput = {
      TableName: this.table,
      ExpressionAttributeNames: projectionNameMappings,
      ProjectionExpression: Object.keys(projectionNameMappings).join(','),
      ...(next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(next, 'base64').toString('ascii'),
            ),
          }
        : {}),
    }
    if(logStatement) {
      console.log(`scanInput: ${JSON.stringify(scanInput, null, 2)}`)
    }
    const result = await this.dynamo
      .scan(scanInput)
      .promise();
    return {
      member: (result.Items ?? []) as T[],
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
        T,
        G[K]['hashKey'],
        G[K]['rangeKey'] extends string ? G[K]['rangeKey'] : null
      >
    : never {
    const indexDef = this.indexes![index];
    return new DynamoTable(
      this.table,
      this.dynamo,
      this.definition,
      indexDef.hashKey as any,
      indexDef.rangeKey as any,
      undefined,
      index as string,
    ) as any;
  }

  async query<P extends (keyof T)[] | null = null>(
    queryParameters: { [K in H]: T[K] } &
      (R extends string
        ? {
            [K in R]?: (
              sortKey: KeyComparisonBuilder<T[R]>,
            ) => any;
          }
        : {}) & {
        filter?: (
          compare: () => ComparisonBuilder<
            Omit<T, R extends string ? H | R : H>
          >,
        ) => CompareWrapperOperator<
          Omit<T, R extends string ? H | R : H>
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
  logStatement?: boolean): Promise<{
    next?: string;
    member: P extends (keyof T)[]
      ? {
          [K in R extends string
            ? P[number] | H | R
            : P[number] | H]: T[K];
        }[]
      : { [K in keyof T]: T[K] }[];
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
    if(logStatement) {
      console.log(`queryInput: ${JSON.stringify(queryInput, null, 2)}`)
    }

    const result = await this.dynamo.query(queryInput).promise();
    return {
      member: (result.Items ?? []) as T[],
      next: result.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
            'base64',
          )
        : undefined,
    } as any;
  }

  private keyPart(
    query: { [K in H]: T[K] } &
      (R extends string
        ? {
            [K in R]?: (
              sortKey: KeyComparisonBuilder<T[R]>,
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
        Omit<T, R extends string ? H | R : H>
      >,
    ) => CompareWrapperOperator<
      Omit<T, R extends string ? H | R : H>
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
    properties: Partial<Omit<T, R extends string ? H | R : H>>,
    increment?: keyof Omit<T, R extends string ? H | R : H>,
    start?: unknown,
  ): {
    UpdateExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues: Record<string, any>;
  } {
    const props = properties as any;
    const validKeys = Object.keys(properties).filter((it) => !!props[it]);
    const removes = Object.keys(properties).filter((it) => !props[it]);
    const hasInc = increment && validKeys.includes(increment as string);
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
        hasInc && start !== undefined ? { ...values, [':start']: start } : values,
    };
  }

  static build<
    D extends DynamoObjectDefinition['object'],
    T extends DynamoEntry<D>,
    H extends keyof DynamoEntry<D>,
    R extends keyof DynamoEntry<D> | null = null,
    G extends Record<
      string,
      { hashKey: keyof DynamoEntry<D>; rangeKey?: keyof DynamoEntry<D> }
    > | null = null,
  >(
    table: string,
    dynamo: DynamoDB.DocumentClient,
    definition: TableEntryDefinition<D, H, R, G>,
  ): DynamoTable<D,DynamoEntry<D>, H, R, G> {
    return new DynamoTable<D, DynamoEntry<D>, H, R, G>(
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
  D extends DynamoObjectDefinition['object'],
  H extends keyof D,
  R extends keyof D | null = null,
  G extends Record<
    string,
    { hashKey: keyof D; rangeKey?: keyof D }
  > | null = null,
> = { definition: D; hashKey: H; rangeKey?: R; indexes?: G };
