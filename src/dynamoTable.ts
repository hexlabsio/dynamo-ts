import { DynamoDB } from 'aws-sdk';
import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import GetItemInput = DocumentClient.GetItemInput;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import QueryInput = DocumentClient.QueryInput;
import WriteRequests = DocumentClient.WriteRequests;
import PutItemInput = DocumentClient.PutItemInput;
import DeleteItemInput = DocumentClient.DeleteItemInput;
import ScanInput = DocumentClient.ScanInput;
import {ComparisonBuilderType, KeyComparisonBuilder, Wrapper} from "./comparison";
import {KeyOperation} from "./operation";
import {DynamoArrayDefinition, DynamoObjectDefinition, TypeFor} from "./type-mapping";
import {nameFor} from "./naming";
import {QueryAllParametersInput, QueryParametersInput} from "./query";
import {DynamoTableIndex} from "./dynamoIndex";
import {DynamoFilter} from "./filter";


export type Increment<T, K extends keyof T> = {
  key: K,
  start?: T[K]
}

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


export class DynamoTable<
  D extends DynamoObjectDefinition['object'],
  T extends DynamoEntry<D>,
  HASH extends keyof T,
  RANGE extends keyof T | null = null,
  PARENT_HASH extends keyof T | null = null, //hash of parent if index
  INDEXES extends Record<
    string,
    { hashKey: keyof T; rangeKey?: keyof T }
  > | null = null,
> 
{
  public readonly tableEntry: T =
    undefined as any;

  private readonly definedKeys: (keyof T)[] = [...[this.rangeKey, this.parentHashKey as keyof T].filter((e): e is keyof T => !!e), this.hashKey]

  protected constructor(
    protected readonly table: string,
    protected readonly dynamo: DynamoDB.DocumentClient,
    private readonly definition: D,
    private readonly hashKey: HASH,
    private readonly rangeKey?: RANGE,
    private readonly parentHashKey?: PARENT_HASH, //hash of parent if index
    private readonly indexes?: INDEXES,
    protected readonly indexName?: string,
    public logStatements: boolean = false
  ) {}

  async get(
    key: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] },
    extras: Omit<GetItemInput, 'TableName' | 'Key'> = {}
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
    if(this.logStatements) {
      console.log(`getInput: ${JSON.stringify(getInput, null, 2)}`)
    }
    const result = await this.dynamo
      .get(getInput)
      .promise();
    return result.Item as T | undefined;
  }
  
  async batchGet<P extends (keyof T)[] | null = null>(keys: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] }[], projection?: P, consistent?: boolean)
  : Promise<P extends (keyof T)[]
    ? { [K in RANGE extends string ? P[number] | HASH | RANGE : P[number] | HASH]: T[K] }[]
    : { [K in keyof T]: T[K] }[]>{
    const actualProjection =  (projection ?? Object.keys(this.definition)) as string[];
    const projectionNameMappings = actualProjection.reduce(
      (acc, it) => ({ ...acc, [`#${nameFor(it as string)}`]: it as string }),
      {},
    );
    const batchGetInput = {RequestItems: {[this.table]: {Keys: keys, ...(projection ? {ProjectionExpression: Object.keys(projectionNameMappings).join(',')} : {}), ...((consistent !== undefined) ? {ConsistentRead: consistent}: {})}}}
    if(this.logStatements) {
      console.log(`batchGetInput: ${JSON.stringify(batchGetInput, null, 2)}`)
    }
    const result = await this.dynamo.batchGet(batchGetInput).promise();
    return result.Responses![this.table] as any;
  }
  
  async directBatchWrite(writeRequest: WriteRequests): Promise<{ unprocessed?: WriteRequests }> {
    const batchWriteInput = {RequestItems: {[this.table]: writeRequest}}
    if(this.logStatements) {
      console.log(JSON.stringify(batchWriteInput, null, 2))
    }
    const result = await this.dynamo.batchWrite(batchWriteInput).promise();
    return { unprocessed: result.UnprocessedItems?.[this.table] };
  }
  
  batchWrite(operations: ({delete: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] }} | {put: T})[]): Promise<{ unprocessed?: WriteRequests }> {
    return this.directBatchWrite(operations.map(operation => {
          return (operation as any).put ? {PutRequest: { Item: (operation as any).put }} : {Delete: { Key: (operation as any).delete }};
    }));
  }
  
  async batchPut(operations: T[]): Promise<void> {
    await this.batchWrite(operations.map(it =>({put: it})))
  }
  
  async batchDelete(operations: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] }[]): Promise<void> {
    await this.batchWrite(operations.map(it =>({delete: it})))
  }

  async put(item: T, extras: Partial<Omit<PutItemInput, 'TableName' | 'Item'>>): Promise<DynamoEntry<D>> {
    const putInput = { TableName: this.table, Item: item, ...extras }
    if(this.logStatements) {
      console.log(`putInput: ${JSON.stringify(putInput, null, 2)}`)
    }
    await this.dynamo.put(putInput).promise();
    return item;
  }

  async delete(
    key: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] },
    extras: Partial<Omit<DeleteItemInput, 'TableName' | 'Key'>>
  ): Promise<void> {
    const deleteInput = { TableName: this.table, Key: key, ...extras }
    if(this.logStatements) {
      console.log(`deleteInput: ${JSON.stringify(deleteInput, null, 2)}`)
    }
    await this.dynamo.delete(deleteInput).promise();
  }

  async update(
    key: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] },
    updates: Partial<Omit<T, RANGE extends string ? HASH | RANGE : HASH>>,
    increments?: Increment<Omit<T, RANGE extends string ? HASH | RANGE : HASH>, keyof Omit<T, RANGE extends string ? HASH | RANGE : HASH>>[],
    extras?: Partial<Omit<UpdateItemInput, 'TableName' | 'Key' | 'UpdateExpression'>>,
  ): Promise<T | undefined> {
    const updateInput = {
      TableName: this.table,
      Key: key,
      ...this.updateExpression(updates, increments),
      ...(extras ?? {}),
    }
    if(this.logStatements) {
      console.log(`updateInput: ${JSON.stringify(updateInput, null, 2)}`)
    }
    const result = await this.dynamo
      .update(updateInput)
      .promise();
    return result.Attributes as T | undefined;
  }

  async updateBatch(
    keys: { [K in RANGE extends string ? HASH | RANGE : HASH]: T[K] }[],
    updates: Partial<Omit<T, RANGE extends string ? HASH | RANGE : HASH>>,
    increments?: Increment<
      Omit<T, RANGE extends string ? HASH | RANGE : HASH>,
      keyof Omit<T, RANGE extends string ? HASH | RANGE : HASH>
    >[],
    extras?: Partial<
      Omit<UpdateItemInput, "TableName" | "Key" | "UpdateExpression">
    >
  ): Promise<(T | undefined)[]> {
    return Promise.all(
      keys.map(async (key) => {
        const updateInput = {
          TableName: this.table,
          Key: key,
          ...this.updateExpression(updates, increments),
          ...(extras ?? {}),
        };
        if (this.logStatements) {
          console.log(`updateInput: ${JSON.stringify(updateInput, null, 2)}`);
        }
        const result = await this.dynamo.update(updateInput).promise();
        return result.Attributes as T | undefined;
      })
    );
  }

  async updateAll<K extends RANGE extends string ? HASH | RANGE : HASH>(
    queryParameters: Omit<
      QueryParametersInput<T, HASH, RANGE, null>,
      "projection" | "dynamo"
    >,
    updates: Partial<Omit<T, RANGE extends string ? HASH | RANGE : HASH>>,
    increments?: Increment<
      Omit<T, RANGE extends string ? HASH | RANGE : HASH>,
      keyof Omit<T, RANGE extends string ? HASH | RANGE : HASH>
    >[],
    extras?: Partial<
      Omit<UpdateItemInput, "TableName" | "Key" | "UpdateExpression">
    >
    
  ): Promise<(T | undefined)[]> {
    const updateKeys = await this.queryAll({
      ...queryParameters,
      projection: this.definedKeys,
    } as QueryAllParametersInput<T, HASH, RANGE, (keyof T)[]>);

    return Promise.all(
      updateKeys.member?.map(async (updateKey) => {
        const updateInput = {
          TableName: this.table,
          Key: updateKey,
          ...this.updateExpression(updates, increments),
          ...(extras ?? {}),
        };
        if (this.logStatements) {
          console.log(`updateInput: ${JSON.stringify(updateInput, null, 2)}`);
        }
        const result = await this.dynamo.update(updateInput).promise();
        return result.Attributes as T | undefined;
      })
    );
  }

  async deleteAll<K extends RANGE extends string ? HASH | RANGE : HASH>(
    queryParameters: Omit<
      QueryParametersInput<T, HASH, RANGE, null>,
      "projection" | "dynamo"
    >
  ): Promise<{ unprocessed?: WriteRequests }[]> {
    const deleteKeys = await this.queryAll({
      ...queryParameters,
      projection: this.definedKeys,
    } as QueryAllParametersInput<T, HASH, RANGE, (keyof T)[]>);

    if (deleteKeys.member && deleteKeys.member.length > 0) {
      const chunkedDeletedKeys = this.chunkArray(deleteKeys.member ?? [], 25);
      return Promise.all(
        chunkedDeletedKeys.map(
          async (batchKeys) =>
            await this.directBatchWrite(
              batchKeys.map((it) => ({ DeleteRequest: { Key: it } }))
            )
        )
      );
    } else {
      return Promise.resolve([]);
    }
  }

  chunkArray<U>(u: U[], chunkSize: number, acc: U[][] = []): U[][] {
    acc.push(u.slice(0, chunkSize));
    const rest = u.slice(chunkSize, u.length);
    return rest.length > 0 ? this.chunkArray(rest, chunkSize, acc) : acc;
  }
  
  async scan<P extends Array<keyof D> | undefined = undefined>(
    filter?: DynamoFilter<T, HASH, RANGE>,
    projection?: P,
    next?: string,
    extras?: Partial<Omit<ScanInput, 'TableName'>>
  ): Promise<{ member: Array<P extends any[] ? Pick<T, P[number]> : T>; next?: string }> {
    const actualProjection =  (projection ?? Object.keys(this.definition)) as string[];
    const projectionNameMappings = actualProjection.reduce(
      (acc, it) => ({ ...acc, [`#${nameFor(it as string)}`]: it as string }),
      {},
    );
    const filterPart = this.filterPart({filter});
    const scanInput: ScanInput = {
      TableName: this.table,
      ...(filterPart.FilterExpression
          ? { FilterExpression: filterPart.FilterExpression }
          : {}),
      ExpressionAttributeNames: {
        ...filterPart.ExpressionAttributeNames,
        ...projectionNameMappings,
      },
      ExpressionAttributeValues: {
        ...filterPart.ExpressionAttributeValues,
      },
      ProjectionExpression: Object.keys(projectionNameMappings).join(','),
      ...(next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(next, 'base64').toString('ascii'),
            ),
          }
        : {}),
      ...extras
    }
    if(this.logStatements) {
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

  index<K extends keyof INDEXES>(
    index: K,
  ): INDEXES extends {}
    ? DynamoTableIndex<
        T,
        INDEXES[K]['hashKey'],
        INDEXES[K]['rangeKey'] extends string ? INDEXES[K]['rangeKey'] : null
      >
    : never {
    const indexDef = this.indexes![index];
    return new DynamoTable(
      this.table,
      this.dynamo,
      this.definition,
      indexDef.hashKey as any,
      indexDef.rangeKey as any,
      this.hashKey as any,
      undefined,
      index as string,
    ) as any;
  }

  private async _recQuery<P extends (keyof T)[] | null = null>(
    queryParameters: QueryAllParametersInput<T, HASH, RANGE, P>,
    enrichKeysFields: (keyof T)[], 
    accumulation: P extends (keyof T)[]
      ? {[K in P[number]]: T[K] }[]
      : { [K in keyof T]: T[K] }[] = []
    ): Promise<{
    next?: string;
    member: P extends (keyof T)[]
      ? {[K in P[number]]: T[K] }[]
      : { [K in keyof T]: T[K] }[];
  }> {

    const allProjection = queryParameters?.projection ? [...queryParameters.projection!, ...enrichKeysFields] : null
    const res =  await this.query<(keyof T)[] | null>({...queryParameters, projection: allProjection})
    const resLength = res?.member?.length ?? 0
    const accLength = accumulation.length
    const limit = queryParameters.queryLimit ?? 0
    if(limit >  0  && limit <= (accLength + resLength) && resLength >= limit-accLength) {
      const nextKey = this.buildNext(res.member[limit-accLength-1])
      return ({
        member: [...accumulation, ...this.removeKeyFields(res.member ?? [], enrichKeysFields).slice(0, limit-accLength)],
        next: nextKey
      })
    } else if(res.next) {
      return this._recQuery({...queryParameters, next: res.next}, enrichKeysFields, [...accumulation, ...this.removeKeyFields(res.member ?? [], enrichKeysFields)])
    } else {
      return  {
        member: [...accumulation, ...this.removeKeyFields(res.member ?? [], enrichKeysFields)]
      }
    }
  }

  private removeKeyFields(members: { [K in keyof T]: T[K]; }[] | { [K in keyof T]: T[K]; }[], enrichKeysFields: (keyof T)[]) {
    return (members ?? []).map(member => {
      enrichKeysFields.forEach(keyField => delete member[keyField]);
      return member;
    });
  }

  private buildNext(t: T): string {
    return Buffer.from(JSON.stringify({
      ...(this.rangeKey ? {[this.rangeKey as keyof T]: t[this.rangeKey as keyof T]} : {}),
      ...(this.parentHashKey ? {[this.parentHashKey as keyof T]: t[this.parentHashKey as keyof T]} : {}),
      [this.hashKey]: t[this.hashKey]
    })).toString('base64')
  }

  async queryAll<P extends (keyof T)[] | null = null>(queryParameters: QueryAllParametersInput<T, HASH, RANGE, P>): Promise<{
    next?: string;
    member: P extends (keyof T)[]
    ? { [K in P[number]]: T[K] }[]
    : { [K in keyof T]: T[K] }[]
  }> {
    const enrichKeysFields = queryParameters.projection 
      ? this.definedKeys.filter(it => !queryParameters.projection!.includes(it))
      : []
    return await this._recQuery<P>(queryParameters, enrichKeysFields)
  }

  async query<P extends (keyof T)[] | null = null>(queryParameters: QueryParametersInput<T, HASH, RANGE, P>): Promise<{
    next?: string;
    member: P extends (keyof T)[]
      ? {[K in P[number]]: T[K] }[]
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
    if(this.logStatements) {
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
    query: { [K in HASH]: T[K] } &
      (RANGE extends string
        ? {
            [K in RANGE]?: (
              sortKey: KeyComparisonBuilder<T[RANGE]>,
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

  private filterPart(query: { filter?: DynamoFilter<T, HASH, RANGE> }): Pick<
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

  updateExpression<K extends (RANGE extends string ? HASH | RANGE : HASH)> (
    properties: Partial<Omit<T, K>>,
    increment?: Increment<Omit<T, K>, keyof Omit<T, K>>[]
  ): {
    UpdateExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues?: Record<string, any>;
  } {
    const props = properties as any;
    const validKeys = Object.keys(properties).filter((it) => props[it] !== undefined);
    const removes = Object.keys(properties).filter((it) => props[it] === undefined);
    function update(key: string, name: string) {
      const inc = (increment ?? []).find(it => it.key === key);
      if(inc) return `#${name} = ` + (inc.start !== undefined ? `if_not_exists(#${name}, :${name}start)`: `#${name}`) + ` + :${name}`
      return `#${name} = :${name}`;
    }
    const setExpression = validKeys.length > 0
      ? `SET ${validKeys.map((key) => update(key, nameFor(key))).filter(it => !!it).join(', ')}` : undefined
    const removeExpression = removes.length > 0
      ? `REMOVE ${removes.map((key) => `#${nameFor(key)}`).join(', ')}` : undefined
    const updateExpression = [setExpression, removeExpression].filter(it => !!it).join(' ')
      
    const names = [...validKeys, ...removes].reduce(
      (names, key) => ({ ...names, [`#${nameFor(key)}`]: key }),
      {},
    );
    const values = validKeys.reduce(
      (values, key) => ({ ...values, [`:${nameFor(key)}`]: props[key] }),
      {},
    );
    const starts = (increment ?? []).filter(it => it.start !== undefined).reduce((acc, increment) =>
      ({...acc, [`:${nameFor(increment.key as string)}start`]: increment.start}), {})
    return {
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: (increment?.length ?? 0) + (validKeys.length) > 0
        ? { ...values, ...starts }
        : undefined
    };
  }

  static build<
    D extends DynamoObjectDefinition['object'],
    T extends DynamoEntry<D>,
    H extends keyof DynamoEntry<D>,
    R extends keyof DynamoEntry<D> | null = null,
    PH extends keyof DynamoEntry<D> | null = null,
    G extends Record<
      string,
      { hashKey: keyof DynamoEntry<D>; rangeKey?: keyof DynamoEntry<D> }
    > | null = null,
  >(
    table: string,
    dynamo: DynamoDB.DocumentClient,
    definition: TableEntryDefinition<D, H, R, G>,
  ): DynamoTable<D,DynamoEntry<D>, H, R, PH, G> {
    return new DynamoTable<D, DynamoEntry<D>, H, R, PH, G>(
      table,
      dynamo,
      definition.definition,
      definition.hashKey,
      definition.rangeKey,
      undefined,
      definition.indexes,
      undefined,
    );
  }
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
