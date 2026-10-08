import { ConsumedCapacity } from '@aws-sdk/client-dynamodb';
import { UpdateCommandInput } from '@aws-sdk/lib-dynamodb';
import {
  BatchWriteClient,
  BatchWriteExecutor,
  BatchWriteItemOptions,
} from '../dynamo-batch-writer.js';
import {
  DeleteItemOptions,
  DeleteItemReturn,
  DeleteReturnValues,
} from '../dynamo-deleter';
import { GetItemOptions, GetItemReturn } from '../dynamo-getter.js';
import {
  PutItemOptions,
  PutItemReturn,
  PutReturnValues,
} from '../dynamo-puter.js';
import { QuerierInput, QuerierReturn } from '../dynamo-querier.js';
import { ScanOptions, ScanReturn } from '../dynamo-scanner.js';
import {
  BatchGetExecutor,
  BatchGetItemOptions,
} from '../dynamo-batch-getter.js';
import {
  ReturnTypesFor,
  TransactGetExecutor,
  TransactGetItemOptions,
  TypeOrProjection,
} from '../dynamo-transact-getter.js';
import { TransactWriteExecutor } from '../dynamo-transact-writer.js';
import { UpdateResult } from '../dynamo-updater.js';
import { TableClient } from '../table-client.js';
import { CamelCaseKeys } from '../types/camel-case.js';
import { DynamoFilter } from '../types/filter.js';
import { JsonPath, ValueAtJsonPath } from '../types/json-path.js';
// Type only: table-definition imports this module at runtime to build clients
import type { TableDefinition, ValidKeys } from './table-definition.js';

type TablePart<T> = {
  partitions: (keyof T & string)[];
  sorts: (keyof T & string)[];
};

type SortKeys<
  KEYS extends (string | number | symbol)[],
  O extends (string | number | symbol)[] = KEYS,
> = KEYS extends [infer A extends string]
  ? { [K in A]: (key: string | number) => any } & {
      [K in O[number]]?: string;
    }
  : KEYS extends [infer A extends string, ...infer TAIL extends string[]]
  ? { [K in A]: (key: string | number) => SortKeys<TAIL, O> } & {
      [K in O[number]]?: string;
    }
  : never;

export type TablePartClients<Parts, D extends TableDefinition> = {
  [P in Parts as P extends TablePartInfo<any, any, infer C, any>
    ? C & string
    : never]: P extends TablePartInfo<infer A, infer B, any, any>
    ? TablePartClient<A, B, P, D>
    : never;
};

/**
 * The key attributes of an index, in order. Values are written as #NAME$value segments.
 */
export type IndexKeys = {
  partition: readonly string[];
  sort: readonly string[];
  /** Local indexes share the table's partition key, so they have no partition attributes of their own */
  local: boolean;
};

type PartIndexes<Info> = Info extends TablePartInfo<any, any, any, any, infer I>
  ? I
  : {};

type Mutable<T> = T extends readonly any[] ? [...T] : never;

/**
 * Queries one of a part's indexes.
 */
export type PartIndexClient<
  TableType,
  Partition,
  Sort extends readonly string[],
> = {
  query<PROJECTION = null>(
    partition: Partition,
    ...rest: Sort extends readonly []
      ? [options?: QuerierInput<TableType, PROJECTION>]
      : [
          keys?: (keys: SortKeys<Mutable<Sort>>) => any,
          options?: QuerierInput<TableType, PROJECTION>,
        ]
  ): Promise<QuerierReturn<TableType, PROJECTION>>;
  /** Like **query**, reading every page */
  queryAll<PROJECTION = null>(
    partition: Partition,
    ...rest: Sort extends readonly []
      ? [options?: Omit<QuerierInput<TableType, PROJECTION>, 'next'>]
      : [
          keys?: (keys: SortKeys<Mutable<Sort>>) => any,
          options?: Omit<QuerierInput<TableType, PROJECTION>, 'next'>,
        ]
  ): Promise<Omit<QuerierReturn<TableType, PROJECTION>, 'next'>>;
};

/**
 * Global indexes are queried by their partition attributes, local indexes by the part's own partition.
 */
type IndexPartition<TableType, T extends TablePart<any>, Keys> = Keys extends {
  local: true;
}
  ? PartKeyValues<TableType, T['partitions'][number]>
  : Keys extends IndexKeys
  ? {
      [K in Keys['partition'][number] & keyof TableType]: NonNullable<
        TableType[K]
      >;
    }
  : never;

type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (
  x: infer I,
) => void
  ? I
  : never;

type PrimaryAttributes<T extends TablePart<any>> =
  | T['partitions'][number]
  | T['sorts'][number];

/**
 * Attributes that can be updated: anything except the attributes that make up the item's key.
 */
export type PartUpdates<TableType, KeyAttributes extends keyof any> = {
  [K in JsonPath<Omit<TableType, KeyAttributes>>]?: ValueAtJsonPath<
    K,
    Omit<TableType, KeyAttributes>
  >;
};

/**
 * Updating any attribute an index uses means providing the rest of its attributes too, so the index keys can be
 * rebuilt without reading the item. Attributes in the item's key are already known.
 */
export type IndexUpdateRequirements<TableType, U, Indexes, KeyAttributes> =
  UnionToIntersection<
    {
      [I in keyof Indexes]: Indexes[I] extends IndexKeys
        ? Exclude<
            Indexes[I]['partition'][number] | Indexes[I]['sort'][number],
            KeyAttributes
          > extends infer Others
          ? [Extract<keyof U, Others>] extends [never]
            ? {}
            : {
                [K in Exclude<Others, keyof U> & keyof TableType]:
                  | TableType[K]
                  | undefined;
              }
          : {}
        : {};
    }[keyof Indexes]
  >;

type NumericAttributes<TableType, KeyAttributes extends keyof any> = {
  [K in keyof Omit<TableType, KeyAttributes>]-?: NonNullable<
    Omit<TableType, KeyAttributes>[K]
  > extends number
    ? K
    : never;
}[keyof Omit<TableType, KeyAttributes>] &
  string;

/**
 * What an update changes, shared by updates and transactional updates.
 */
export type PartUpdateFields<
  TableType,
  KeyAttributes extends keyof any,
  Indexes,
  U,
> = {
  /** The attributes to set. Undefined values are removed. */
  updates: U &
    IndexUpdateRequirements<TableType, U, Indexes, KeyAttributes> & {
      [K in Exclude<
        keyof U,
        keyof PartUpdates<TableType, KeyAttributes>
      >]: never;
    };
  condition?: DynamoFilter<TableType>;
  /** Attributes to add the update value to atomically, starting from **start** if they don't exist. */
  increments?: Array<{
    key: NumericAttributes<TableType, KeyAttributes>;
    start?: number;
  }>;
};

export type PartUpdateOptions<
  TableType,
  KeyAttributes extends keyof any,
  Indexes,
  U,
  RETURN,
> = Partial<
  CamelCaseKeys<
    Pick<
      UpdateCommandInput,
      'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'
    >
  >
> &
  PartUpdateFields<TableType, KeyAttributes, Indexes, U> & {
    return?: RETURN;
  };

/**
 * Values for some of a part's key attributes, with their types from the part.
 */
export type PartKeyValues<TableType, Keys extends keyof any> = {
  [K in Keys & keyof TableType]: NonNullable<TableType[K]>;
};

type PartKey<TableType, T extends TablePart<any>> = PartKeyValues<
  TableType,
  PrimaryAttributes<T>
>;

type ConditionFailureOption = {
  /** Return the item's attributes in the error if the condition fails */
  returnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE';
};

/**
 * Transactional requests for a part. Combine them with **then** (writes) or **and** (gets), including with requests
 * for other parts and tables, then call **execute**.
 */
export type PartTransactions<TableType, T extends TablePart<any>, Indexes> = {
  put(
    options: {
      item: TableType;
      condition?: DynamoFilter<TableType>;
    } & ConditionFailureOption,
  ): TransactWriteExecutor;
  update<const U extends PartUpdates<TableType, PrimaryAttributes<T>>>(
    options: { key: PartKey<TableType, T> } & PartUpdateFields<
      TableType,
      PrimaryAttributes<T>,
      Indexes,
      U
    > &
      ConditionFailureOption,
  ): TransactWriteExecutor;
  delete(
    options: {
      key: PartKey<TableType, T>;
      condition?: DynamoFilter<TableType>;
    } & ConditionFailureOption,
  ): TransactWriteExecutor;
  conditionCheck(
    options: {
      key: PartKey<TableType, T>;
      condition: DynamoFilter<TableType>;
    } & ConditionFailureOption,
  ): TransactWriteExecutor;
  get<const K extends PartKey<TableType, T>[], PROJECTION = null>(
    keys: K,
    options?: TransactGetItemOptions<TableType, PROJECTION>,
  ): TransactGetExecutor<
    ReturnTypesFor<K, TypeOrProjection<TableType, PROJECTION>>
  >;
};

export type UpdateItemReturnSingleTable<
  BaseDefinition extends TableDefinition,
  TableType,
  RETURN extends UpdateCommandInput['ReturnValues'] | null,
> = UpdateResult<TableType, RETURN> & { keys: BaseDefinition['type'] };

export type ParentTypes<T extends any[]> = T extends [infer A]
  ? A
  : T extends [infer A, ...infer Rest]
  ? { item: A; member: ParentTypes<Rest>[] }
  : never;
export type ParentType<
  P extends TablePartInfo<any, any, any, any, any> | null,
  Depth extends 0[] = [],
> = Depth['length'] extends 5
  ? []
  : P extends TablePartInfo<infer A, any, any, infer PP>
  ? PP extends null
    ? [A]
    : [...ParentType<PP, [...Depth, 0]>, A]
  : [];

export type CombinedTypes<
  P extends TablePartInfo<any, any, any, any, any> | null,
  Depth extends 0[] = [],
> = Depth['length'] extends 5
  ? {}
  : P extends TablePartInfo<infer A, any, any, infer PP>
  ? PP extends null
    ? A
    : A & CombinedTypes<PP, [...Depth, 0]>
  : {};

export type PutItemReturnSingleTable<
  BaseDefinition extends TableDefinition,
  TableType,
  RETURN extends PutReturnValues,
> = PutItemReturn<TableType, RETURN> & { keys: BaseDefinition['type'] };

export type GetItemReturnSingleTable<
  BaseDefinition extends TableDefinition,
  TableType,
  PROJECTION,
> = GetItemReturn<TableType, PROJECTION> & { keys: BaseDefinition['type'] };

export type DeleteItemReturnSingleTable<
  BaseDefinition extends TableDefinition,
  TableType,
  RETURN extends DeleteReturnValues,
> = DeleteItemReturn<TableType, RETURN> & { keys: BaseDefinition['type'] };

export class TablePartClient<
  TableType,
  T extends TablePart<TableType>,
  Info extends TablePartInfo<any, any, any, any, any>,
  Definition extends TableDefinition,
> {
  constructor(
    private readonly part: T,
    private readonly parent: Info,
    private readonly prefix: string,
    private readonly tableClient: TableClient<Definition>,
  ) {}
  private proxySetter(set: (name: string, value: string) => void) {
    const self = this;
    return new Proxy(
      {},
      {
        get(target, name) {
          return (value: string) => {
            set(name as any, value);
            return self.proxySetter(set);
          };
        },
      },
    );
  }

  getParentChain(
    info: TablePartInfo<any, any, any, any, any> = this.parent,
  ): TablePartInfo<any, any, any, any, any>[] {
    if (info.parents) return [...this.getParentChain(info.parents), info];
    return [info];
  }

  private keySegments(keys: string[], values: Record<string, any>): string {
    return keys.reduce(
      (prev, next) => `${prev}#${next.toUpperCase()}$${values[next]}`,
      '',
    );
  }

  private get keyNames(): { partitionKey: string; sortKey: string } {
    return this.tableClient.tableConfig.keyNames as any;
  }

  /**
   * Joined parts start their sort key with the part name so they can be told apart from their parent.
   */
  private get sortPrefix(): string {
    return this.parent.parents ? `#${this.prefix.toUpperCase()}` : '';
  }

  private primaryKeys(item: Record<string, any>): Record<string, string> {
    return {
      [this.keyNames.partitionKey]: this.keySegments(
        this.part.partitions,
        item,
      ),
      [this.keyNames.sortKey]: `${this.sortPrefix}${this.keySegments(
        this.part.sorts,
        item,
      )}`,
    };
  }

  /**
   * Index keys for every index whose attributes are all present on the item. Indexes with missing attributes are left
   * off, so the item doesn't appear in them.
   */
  private indexAttributes(item: Record<string, any>): Record<string, string> {
    const indexes: Record<string, IndexKeys> = this.parent.indexes ?? {};
    return Object.entries(indexes).reduce((attributes, [name, keys]) => {
      const missing = [...keys.partition, ...keys.sort].some(
        (key) => item[key] === undefined || item[key] === null,
      );
      if (missing) return attributes;
      const keyNames = (this.tableClient.tableConfig.indexes as any)[name];
      return { ...attributes, ...this.indexKeyValues(keys, keyNames, item) };
    }, {});
  }

  /**
   * The index key attributes for an item with all of the index's attributes. Local indexes only have a sort key, which
   * starts with the part name.
   */
  private indexKeyValues(
    keys: IndexKeys,
    keyNames: { partitionKey: string; sortKey?: string },
    item: Record<string, any>,
  ): Record<string, string> {
    const typePrefix = `#${this.prefix.toUpperCase()}`;
    if (keys.local) {
      return {
        [keyNames.sortKey!]: `${typePrefix}${this.keySegments(
          [...keys.sort],
          item,
        )}`,
      };
    }
    return {
      [keyNames.partitionKey]: `${typePrefix}${this.keySegments(
        [...keys.partition],
        item,
      )}`,
      ...(keys.sort.length
        ? { [keyNames.sortKey!]: this.keySegments([...keys.sort], item) }
        : {}),
    };
  }

  private withKeys(item: Record<string, any>): Record<string, any> {
    return {
      ...item,
      ...this.indexAttributes(item),
      ...this.primaryKeys(item),
    };
  }

  private sortKeyValues(
    keys: ((keys: any) => any) | undefined,
  ): Record<string, string | number> {
    const values: Record<string, string | number> = {};
    keys?.(
      this.proxySetter((name: string, value: string) => {
        values[name] = value;
      }),
    );
    return values;
  }

  /**
   * A precise sort key condition: equality when every key is given, otherwise begins_with on the given keys followed by
   * a separator, so o-1 does not match o-10.
   */
  private sortCondition(
    sorts: readonly string[],
    values: Record<string, string | number>,
    prefix: string,
    whenEmpty: string | undefined,
  ): ((sortKey: any) => any) | undefined {
    const given: string[] = [];
    for (const key of sorts) {
      if (values[key] === undefined) break;
      given.push(key);
    }
    const value = `${prefix}${this.keySegments(given, values)}`;
    if (given.length === sorts.length && given.length > 0)
      return (sortKey) => sortKey.eq(value);
    if (given.length > 0) return (sortKey) => sortKey.beginsWith(`${value}#`);
    return whenEmpty === undefined
      ? undefined
      : (sortKey) => sortKey.beginsWith(whenEmpty);
  }

  /**
   * The sort key prefix shared by every item of a joined part that belongs to the given parent item.
   */
  private joinedPrefix(
    info: TablePartInfo<any, any, any, any, any>,
    parentInfo: TablePartInfo<any, any, any, any, any>,
    parent: Record<string, any>,
  ): string {
    return `#${info.prefix.toUpperCase()}${this.keySegments(
      parentInfo.part.sorts,
      parent,
    )}`;
  }

  private sumCapacity(
    capacities: (ConsumedCapacity | undefined)[],
  ): ConsumedCapacity | undefined {
    const consumed = capacities.filter((it): it is ConsumedCapacity => !!it);
    if (!consumed.length) return undefined;
    return {
      TableName: consumed[0].TableName,
      CapacityUnits: consumed.reduce(
        (total, it) => total + (it.CapacityUnits ?? 0),
        0,
      ),
    };
  }

  private intoParentage(
    items: any[],
    chain: { name: string; typePrefix: string }[],
    search: (item: any) => boolean = () => true,
  ): any {
    const [{ name, typePrefix }, ...rest] = chain;
    const results = items.filter(
      (it) =>
        it[this.tableClient.tableConfig.keyNames.sortKey].startsWith(
          typePrefix,
        ) && search(it),
    );
    if (rest.length === 0) return results;
    return results.map((it) => ({
      item: it,
      member: this.intoParentage(
        items,
        rest,
        (o) => search(o) && o[name] === it[name],
      ),
    }));
  }

  async query<PROJECTION = null>(
    partition: PartKeyValues<TableType, T['partitions'][number]>,
    keys?: (keys: SortKeys<T['sorts']>) => any,
    options: QuerierInput<TableType, PROJECTION> = {},
  ): Promise<QuerierReturn<TableType, PROJECTION>> {
    const typePrefix = `#${this.prefix.toUpperCase()}${
      this.parent.parents ? '#' : '$'
    }`;
    const condition = this.sortCondition(
      this.part.sorts,
      this.sortKeyValues(keys),
      this.sortPrefix,
      typePrefix,
    );
    const result = await this.tableClient.query(
      {
        [this.keyNames.partitionKey]: this.keySegments(
          this.part.partitions,
          partition,
        ),
        [this.keyNames.sortKey]: condition,
      } as any,
      options as any,
    );
    return result as any;
  }

  /**
   * Selects one of this part's indexes to query.
   */
  index<N extends keyof PartIndexes<Info> & string>(
    name: N,
  ): PartIndexClient<
    TableType,
    IndexPartition<TableType, T, PartIndexes<Info>[N]>,
    PartIndexes<Info>[N] extends IndexKeys ? PartIndexes<Info>[N]['sort'] : []
  > {
    const keys: IndexKeys = (this.parent.indexes as any)[name];
    if (!keys)
      throw new Error(`Part ${this.prefix} has no index called ${name}`);
    const keyNames = (this.tableClient.tableConfig.indexes as any)[name];
    const typePrefix = `#${this.prefix.toUpperCase()}`;
    const index = {
      queryAll: (partition: Record<string, any>, ...rest: any[]) => {
        const [sortKeys, options] = keys.sort.length
          ? [rest[0], rest[1] ?? {}]
          : [undefined, rest[0] ?? {}];
        return this.allPages((next) =>
          keys.sort.length
            ? index.query(partition, sortKeys, { ...options, next })
            : index.query(partition, { ...options, next }),
        );
      },
      query: async (partition: Record<string, any>, ...rest: any[]) => {
        const [sortKeys, options] = keys.sort.length
          ? [rest[0], rest[1] ?? {}]
          : [undefined, rest[0] ?? {}];
        const values = this.sortKeyValues(sortKeys);
        // Local index sort keys start with the part name, as every part in the partition shares the index
        const condition = keys.local
          ? this.sortCondition(keys.sort, values, typePrefix, `${typePrefix}#`)
          : this.sortCondition(keys.sort, values, '', undefined);
        const partitionValue = keys.local
          ? this.keySegments(this.part.partitions, partition)
          : `${typePrefix}${this.keySegments([...keys.partition], partition)}`;
        return this.tableClient.index(name as any).query(
          {
            [keyNames.partitionKey]: partitionValue,
            ...(condition ? { [keyNames.sortKey]: condition } : {}),
          } as any,
          options,
        );
      },
    };
    return index as any;
  }

  /**
   * Reads every page of a paged request.
   */
  private async allPages<R extends { member: any[]; next?: string }>(
    page: (next: string | undefined) => Promise<R>,
  ): Promise<Omit<R, 'next'>> {
    const member: any[] = [];
    let next: string | undefined;
    let last: R;
    do {
      last = await page(next);
      member.push(...last.member);
      next = last.next;
    } while (next);
    const { next: _, ...rest } = last;
    void _;
    return { ...rest, member, count: member.length } as any;
  }

  /**
   * Like **query**, reading every page.
   */
  queryAll<PROJECTION = null>(
    partition: PartKeyValues<TableType, T['partitions'][number]>,
    keys?: (keys: SortKeys<T['sorts']>) => any,
    options: Omit<QuerierInput<TableType, PROJECTION>, 'next'> = {},
  ): Promise<Omit<QuerierReturn<TableType, PROJECTION>, 'next'>> {
    return this.allPages((next) =>
      this.query(partition, keys, { ...options, next }),
    );
  }

  /**
   * Scans the whole table for this part's items, one page at a time. Scans read (and pay for) every item in the table.
   */
  scan<PROJECTION = null>(
    options: ScanOptions<TableType, PROJECTION> = {},
  ): Promise<ScanReturn<TableType, PROJECTION>> {
    const typePrefix = `#${this.prefix.toUpperCase()}${
      this.parent.parents ? '#' : '$'
    }`;
    const sortKey = this.keyNames.sortKey;
    const filter = (compare: any) => {
      const ofThisPart = compare()[sortKey].beginsWith(typePrefix);
      return options.filter
        ? compare().and(ofThisPart, (options.filter as any)(compare))
        : ofThisPart;
    };
    return this.tableClient.scan({ ...options, filter } as any) as any;
  }

  /**
   * Like **scan**, reading every page.
   */
  scanAll<PROJECTION = null>(
    options: Omit<ScanOptions<TableType, PROJECTION>, 'next'> = {},
  ): Promise<Omit<ScanReturn<TableType, PROJECTION>, 'next'>> {
    return this.allPages((next) => this.scan({ ...options, next }));
  }

  /**
   * Queries the top level parents in the partition along with all of their joined children, grouped into a tree.
   *
   * Paging applies to the top level parents: **limit** caps how many are read per page and **next** continues from
   * the previous page. Every page contains the complete set of children for the parents it returns.
   */
  async queryWithParents<PROJECTION = null>(
    partition: PartKeyValues<TableType, T['partitions'][number]>,
    options: QuerierInput<CombinedTypes<Info>, PROJECTION> = {},
  ): Promise<QuerierReturn<ParentTypes<ParentType<Info>>, PROJECTION>> {
    const { partitionKey, sortKey } = this.tableClient.tableConfig.keyNames;
    const partitionString = this.keySegments(this.part.partitions, partition);
    const chain = this.getParentChain();
    // The chain root is a from or childPart (sort key #NAME$value), the rest are joinParts (#NAME#...)
    const typePrefixes = chain.map(
      (info, index) =>
        `#${info.prefix.toUpperCase()}${index === 0 ? '$' : '#'}`,
    );
    const root = await this.tableClient.query(
      {
        [partitionKey]: partitionString,
        [sortKey]: (sk: any) => sk.beginsWith(typePrefixes[0]),
      } as any,
      options as any,
    );
    const items: any[] = [...root.member];
    const capacities = [root.consumedCapacity];
    let parents: any[] = root.member;
    for (let level = 1; level < chain.length && parents.length > 0; level++) {
      const info = chain[level];
      const parentInfo = chain[level - 1];
      const prefixes = new Set(
        parents.map((parent) => this.joinedPrefix(info, parentInfo, parent)),
      );
      // DynamoDB orders strings by their UTF-8 bytes, so the children of these parents all fall in this range
      const sorted = [...prefixes].sort((a, b) =>
        Buffer.compare(Buffer.from(a), Buffer.from(b)),
      );
      const from = sorted[0];
      const to = `${sorted[sorted.length - 1]}\u{10FFFF}`;
      const children: any[] = [];
      let page: string | undefined;
      do {
        const result = await this.tableClient.query(
          {
            [partitionKey]: partitionString,
            [sortKey]: (sk: any) => sk.between(from, to),
          } as any,
          { ...options, limit: undefined, next: page } as any,
        );
        capacities.push(result.consumedCapacity);
        children.push(
          ...result.member.filter((child: any) =>
            prefixes.has(this.joinedPrefix(info, parentInfo, child)),
          ),
        );
        page = result.next;
      } while (page);
      items.push(...children);
      parents = children;
    }
    const member = this.intoParentage(
      items,
      chain.map((info, index) => ({
        name: info.prefix,
        typePrefix: typePrefixes[index],
      })),
    );
    return {
      ...root,
      member,
      consumedCapacity: this.sumCapacity(capacities),
    } as any;
  }

  /**
   * Queries every top level parent in the partition along with all of their joined children, grouped into a tree.
   *
   * Reads all pages of **queryWithParents** and combines them.
   */
  async queryAllWithParents<PROJECTION = null>(
    partition: PartKeyValues<TableType, T['partitions'][number]>,
    options: Omit<
      QuerierInput<CombinedTypes<Info>, PROJECTION>,
      'next' | 'limit'
    > = {},
  ): Promise<
    Omit<QuerierReturn<ParentTypes<ParentType<Info>>, PROJECTION>, 'next'>
  > {
    const member: any[] = [];
    const capacities: (ConsumedCapacity | undefined)[] = [];
    let next: string | undefined;
    do {
      const page = await this.queryWithParents(partition, {
        ...options,
        next,
      });
      member.push(...page.member);
      capacities.push(page.consumedCapacity);
      next = page.next;
    } while (next);
    return {
      member,
      count: member.length,
      consumedCapacity: this.sumCapacity(capacities),
    } as any;
  }

  async put<RETURN extends PutReturnValues = 'NONE'>(
    item: TableType,
    options: PutItemOptions<TableType, RETURN> = {},
  ): Promise<PutItemReturnSingleTable<Definition, TableType, RETURN>> {
    const keys: Definition['type'] = this.primaryKeys(item as any);
    const putResult = (await this.tableClient.put(
      this.withKeys(item as any),
      options as any,
    )) as any;
    return { ...putResult, keys };
  }

  /**
   * Updates an item, or creates it if it doesn't exist.
   *
   * Changing an attribute that an index uses means providing the rest of that index's attributes too, so its keys can
   * be rebuilt. Setting one of them to undefined removes the item from the index.
   */
  async update<
    const U extends PartUpdates<TableType, PrimaryAttributes<T>>,
    RETURN extends UpdateCommandInput['ReturnValues'] | null = null,
  >(
    key: PartKey<TableType, T>,
    options: PartUpdateOptions<
      TableType,
      PrimaryAttributes<T>,
      PartIndexes<Info>,
      U,
      RETURN
    >,
  ): Promise<UpdateItemReturnSingleTable<Definition, TableType, RETURN>> {
    const request = this.updateRequest(key, options);
    const result = await this.tableClient.update(request as any);
    return { ...result, keys: request.key } as any;
  }

  /**
   * Turns a part update into a table update: checks key attributes aren't changed, keeps the identifying attributes
   * and maintains index keys.
   */
  private updateRequest(
    key: Record<string, any>,
    options: Record<string, any>,
  ): Record<string, any> & { key: Definition['type'] } {
    const { updates, ...rest } = options;
    const keyAttribute = [...this.part.partitions, ...this.part.sorts].find(
      (it) => it in updates,
    );
    if (keyAttribute)
      throw new Error(
        `${keyAttribute} is part of the key for ${this.prefix} and can't be updated`,
      );
    return {
      ...rest,
      key: this.primaryKeys(key),
      updates: {
        // Keeps the identifying attributes on the item when the update creates it
        ...key,
        ...updates,
        ...this.indexUpdates(key, updates, rest.increments ?? []),
      },
    };
  }

  /**
   * Transactional reads and writes for this part.
   */
  get transaction(): PartTransactions<TableType, T, PartIndexes<Info>> {
    const transaction = this.tableClient.transaction as any;
    return {
      put: ({ item, ...rest }: any) =>
        transaction.put({ ...rest, item: this.withKeys(item) }),
      update: ({ key, ...rest }: any) =>
        transaction.update(this.updateRequest(key, rest)),
      delete: ({ key, ...rest }: any) =>
        transaction.delete({ ...rest, key: this.primaryKeys(key) }),
      conditionCheck: ({ key, ...rest }: any) =>
        transaction.conditionCheck({ ...rest, key: this.primaryKeys(key) }),
      get: (keys: Record<string, any>[], options?: any) =>
        transaction.get(
          keys.map((key) => this.primaryKeys(key)),
          options,
        ),
    } as any;
  }

  /**
   * The index key attributes to set or remove for an update.
   */
  private indexUpdates(
    key: Record<string, any>,
    updates: Record<string, any>,
    increments: { key: string }[],
  ): Record<string, string | undefined> {
    const keyAttributes: string[] = [
      ...this.part.partitions,
      ...this.part.sorts,
    ];
    const incremented = increments.map((it) => it.key);
    const indexes: Record<string, IndexKeys> = this.parent.indexes ?? {};
    return Object.entries(indexes).reduce((attributes, [name, index]) => {
      const used = [...index.partition, ...index.sort];
      const increment = used.find((it) => incremented.includes(it));
      if (increment)
        throw new Error(
          `Cannot increment ${increment} because index ${name} uses it`,
        );
      const others = used.filter((it) => !keyAttributes.includes(it));
      const changed = others.filter((it) => it in updates);
      if (others.length > 0 && changed.length === 0) return attributes;
      const missing = others.filter((it) => !(it in updates));
      if (missing.length > 0)
        throw new Error(
          `Updating ${changed.join(
            ', ',
          )} changes index ${name}, so ${missing.join(
            ', ',
          )} must also be provided`,
        );
      const values = { ...key, ...updates };
      const complete = used.every(
        (it) => values[it] !== undefined && values[it] !== null,
      );
      const keyNames = (this.tableClient.tableConfig.indexes as any)[name];
      if (complete) {
        return {
          ...attributes,
          ...this.indexKeyValues(index, keyNames, values),
        };
      }
      return {
        ...attributes,
        ...(index.local ? {} : { [keyNames.partitionKey]: undefined }),
        ...(index.sort.length ? { [keyNames.sortKey]: undefined } : {}),
      };
    }, {});
  }

  batchPut(
    items: TableType[],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    return this.tableClient.batchPut(
      items.map((item) => this.withKeys(item as any)),
      options as any,
    );
  }

  /**
   * Gets up to 100 items in one request. Combine with other batch gets, including for other parts, with **and()**.
   */
  batchGet<PROJECTION = null>(
    keys: PartKey<TableType, T>[],
    options: BatchGetItemOptions<TableType, PROJECTION> = {},
  ): BatchGetExecutor<TableType, PROJECTION> {
    return this.tableClient.batchGet(
      keys.map((key) => this.primaryKeys(key)) as any,
      options as any,
    ) as any;
  }

  /**
   * Deletes up to 25 items in one request. Combine with other batch writes, including for other parts, with **and()**.
   */
  batchDelete(
    keys: PartKey<TableType, T>[],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    return this.tableClient.batchDelete(
      keys.map((key) => this.primaryKeys(key)) as any,
      options,
    );
  }

  async get<PROJECTION = null>(
    item: PartKey<TableType, T>,
    options: GetItemOptions<TableType, PROJECTION> = {},
  ): Promise<GetItemReturnSingleTable<Definition, TableType, PROJECTION>> {
    const keys: Definition['type'] = this.primaryKeys(item);
    const result = (await this.tableClient.get(
      { ...keys },
      options as any,
    )) as any;
    return { ...result, keys };
  }

  async delete<RETURN extends DeleteReturnValues>(
    item: PartKey<TableType, T>,
    options: DeleteItemOptions<TableType, RETURN> = {},
  ): Promise<DeleteItemReturnSingleTable<Definition, TableType, RETURN>> {
    const keys: Definition['type'] = this.primaryKeys(item);
    const result = (await this.tableClient.delete(
      { ...keys },
      options as any,
    )) as any;
    return { ...result, keys };
  }
}

export class TablePartInfo<
  TableType,
  T extends TablePart<TableType>,
  NAME extends keyof any,
  Parent extends TablePartInfo<any, any, any, any, any> | null = null,
  Indexes = {},
> {
  constructor(
    public readonly part: T,
    public readonly parents: Parent,
    public readonly prefix: string,
    public readonly indexes: Indexes = {} as Indexes,
  ) {}
}

type AnyPart = TablePartInfo<any, any, any, any, any>;

// Extract keeps the key lists unchanged, while telling the compiler they are keys of the part's type
type KeysOf<J, X> = Extract<X, (keyof J & string)[]>;

export type RootPart<
  TableType,
  K extends string,
  K2 extends string,
  Indexes = {},
> = TablePartInfo<
  TableType,
  { partitions: KeysOf<TableType, [K]>; sorts: KeysOf<TableType, [K2]> },
  K2,
  null,
  Indexes
>;

export type JoinedPart<
  Parent extends AnyPart,
  J,
  K extends string,
  Indexes = {},
> = Parent extends TablePartInfo<any, infer T, any, any>
  ? TablePartInfo<
      J,
      {
        partitions: KeysOf<J, T['partitions']>;
        sorts: KeysOf<J, [...T['sorts'], K]>;
      },
      K,
      Parent,
      Indexes
    >
  : never;

export type ChildPart<
  Parent extends AnyPart,
  J,
  K extends string,
  Indexes = {},
> = Parent extends TablePartInfo<any, infer T, any, any>
  ? TablePartInfo<
      J,
      {
        partitions: KeysOf<J, [...T['partitions'], ...T['sorts']]>;
        sorts: KeysOf<J, [K]>;
      },
      K,
      null,
      Indexes
    >
  : never;

/*
 * Single table schema
 *
 * A single table is described as a tree of nodes, where each property name is the name of a part and the attribute
 * used as its key.
 */

type NodeKind = 'part' | 'join' | 'child';

type KeyAttribute<TableType> = ValidKeys<TableType> & string;

// Index attributes may be optional: items without them are left out of the index
type IndexAttribute<TableType> = {
  [K in keyof TableType]-?: NonNullable<TableType[K]> extends string | number
    ? K
    : never;
}[keyof TableType] &
  string;

export interface SchemaNode<
  Kind extends NodeKind,
  TableType,
  PK extends string,
  Children,
  Indexes,
> {
  readonly kind: Kind;
  readonly partitionKey: PK;
  readonly children: Children;
  readonly indexes: Indexes;
  /** Type only */
  readonly tableType?: TableType;

  /**
   * Adds this part to an index. The index partition key is the part name followed by the **partition** attributes,
   * and its sort key is the **sort** attributes, in order.
   *
   * Items missing any of these attributes are left out of the index.
   */
  index<
    const N extends string,
    const P extends readonly IndexAttribute<TableType>[],
    const S extends readonly [
      IndexAttribute<TableType>,
      ...IndexAttribute<TableType>[],
    ],
  >(
    name: Exclude<N, keyof Indexes>,
    keys: { partition: P; sort: S },
  ): SchemaNode<
    Kind,
    TableType,
    PK,
    Children,
    Indexes & { [K in N]: { partition: P; sort: S; local: false } }
  >;
  index<
    const N extends string,
    const P extends readonly IndexAttribute<TableType>[],
  >(
    name: Exclude<N, keyof Indexes>,
    keys: { partition: P },
  ): SchemaNode<
    Kind,
    TableType,
    PK,
    Children,
    Indexes & { [K in N]: { partition: P; sort: []; local: false } }
  >;

  /**
   * Adds this part to a local index: the table's partition key, sorted by the **sort** attributes in order. Local
   * indexes support strongly consistent reads, but can only be created with the table and limit each partition key
   * value to 10 GB.
   */
  localIndex<
    const N extends string,
    const S extends readonly [
      IndexAttribute<TableType>,
      ...IndexAttribute<TableType>[],
    ],
  >(
    name: Exclude<N, keyof Indexes>,
    keys: { sort: S },
  ): SchemaNode<
    Kind,
    TableType,
    PK,
    Children,
    Indexes & { [K in N]: { partition: []; sort: S; local: true } }
  >;

  /**
   * The parts nested under this one.
   */
  with<const C extends SchemaChildren>(
    children: C,
  ): SchemaNode<Kind, TableType, PK, C, Indexes>;
}

export type SchemaChildren = Record<
  string,
  SchemaNode<'join' | 'child', any, any, any, any>
>;

export type SingleTableSchema = Record<
  string,
  SchemaNode<'part', any, any, any, any>
>;

export type SingleTableHelpers = {
  /**
   * A root part. Its partition key is the attribute passed to **partitionedBy**, and its sort key is the property name.
   */
  part<TableType>(): {
    partitionedBy<const PK extends KeyAttribute<TableType>>(
      partitionKey: PK,
    ): SchemaNode<'part', TableType, PK, {}, {}>;
  };
  /**
   * A part that shares its parent's partition, so it can be read together with the parent. Use it for children that
   * are small or bounded.
   */
  join<TableType>(): SchemaNode<'join', TableType, never, {}, {}>;
  /**
   * A part that gets its own partition under its parent. Use it for children that can grow without bound.
   */
  child<TableType>(): SchemaNode<'child', TableType, never, {}, {}>;
};

function schemaNode(
  kind: NodeKind,
  partitionKey: string | undefined,
  children: SchemaChildren,
  indexes: Record<string, IndexKeys>,
): SchemaNode<any, any, any, any, any> {
  return {
    kind,
    partitionKey,
    children,
    indexes,
    index: (
      name: string,
      keys: { partition: readonly string[]; sort?: readonly string[] },
    ) => {
      if (indexes[name])
        throw new Error(`A part can only use index ${name} once`);
      return schemaNode(kind, partitionKey, children, {
        ...indexes,
        [name]: {
          partition: [...keys.partition],
          sort: [...(keys.sort ?? [])],
          local: false,
        },
      });
    },
    localIndex: (name: string, keys: { sort: readonly string[] }) => {
      if (indexes[name])
        throw new Error(`A part can only use index ${name} once`);
      return schemaNode(kind, partitionKey, children, {
        ...indexes,
        [name]: { partition: [], sort: [...keys.sort], local: true },
      });
    },
    with: (newChildren: SchemaChildren) =>
      schemaNode(kind, partitionKey, newChildren, indexes),
  } as any;
}

export const singleTableHelpers: SingleTableHelpers = {
  part: () => ({
    partitionedBy: (partitionKey: string) =>
      schemaNode('part', partitionKey, {}, {}),
  }),
  join: () => schemaNode('join', undefined, {}, {}),
  child: () => schemaNode('child', undefined, {}, {}),
} as any;

type SchemaError<Message extends string> = `Error: ${Message}`;

type ValidateChildren<Children, ParentType, ParentKeys extends string> = {
  [N in keyof Children]: Children[N] extends SchemaNode<
    'join' | 'child',
    infer T,
    any,
    infer C,
    any
  >
    ? T extends Pick<ParentType, ParentKeys & keyof ParentType>
      ? N extends KeyAttribute<T>
        ? Children[N] & { children: ValidateChildren<C, T, ParentKeys | N> }
        : SchemaError<`${N &
            string} must be a string or number attribute of its part`>
      : SchemaError<`${N &
          string} must have all the key attributes of its parent`>
    : SchemaError<`${N & string} must be created with join() or child()`>;
};

/**
 * Maps any invalid node in the schema to an error message, so the mistake is reported on that property.
 */
export type ValidateSchema<S> = {
  [N in keyof S]: S[N] extends SchemaNode<
    'part',
    infer T,
    infer PK,
    infer C,
    any
  >
    ? N extends KeyAttribute<T>
      ? N extends PK
        ? SchemaError<`${N & string} can't be both the partition and sort key`>
        : S[N] & { children: ValidateChildren<C, T, PK | N> }
      : SchemaError<`${N &
          string} must be a string or number attribute of its part`>
    : SchemaError<`${N & string} must be created with part()`>;
};

type FlattenChildren<Children, Parent extends AnyPart> = {
  [N in keyof Children & string]: Children[N] extends SchemaNode<
    'join',
    infer T,
    any,
    infer C,
    infer I
  >
    ?
        | JoinedPart<Parent, T, N, I>
        | FlattenChildren<C, JoinedPart<Parent, T, N, I>>
    : Children[N] extends SchemaNode<'child', infer T, any, infer C, infer I>
    ?
        | ChildPart<Parent, T, N, I>
        | FlattenChildren<C, ChildPart<Parent, T, N, I>>
    : never;
}[keyof Children & string];

/**
 * Every part in the schema, as a union.
 */
export type SchemaParts<S> = {
  [N in keyof S & string]: S[N] extends SchemaNode<
    'part',
    infer T,
    infer PK,
    infer C,
    infer I
  >
    ? RootPart<T, PK, N, I> | FlattenChildren<C, RootPart<T, PK, N, I>>
    : never;
}[keyof S & string];

/**
 * Builds the part definitions for a schema, parents before their children.
 */
export function schemaParts(schema: SingleTableSchema): AnyPart[] {
  const children = (nodes: SchemaChildren, parent: AnyPart): AnyPart[] =>
    Object.entries(nodes).flatMap(([name, node]) => {
      const part =
        node.kind === 'join'
          ? new TablePartInfo<any, any, any, any, any>(
              {
                partitions: parent.part.partitions,
                sorts: [...parent.part.sorts, name],
              },
              parent,
              name,
              node.indexes,
            )
          : new TablePartInfo<any, any, any, any, any>(
              {
                partitions: [...parent.part.partitions, ...parent.part.sorts],
                sorts: [name],
              },
              null,
              name,
              node.indexes,
            );
      return [part, ...children(node.children, part)];
    });
  return Object.entries(schema).flatMap(([name, node]) => {
    const root = new TablePartInfo<any, any, any, any, any>(
      { partitions: [node.partitionKey], sorts: [name] },
      null,
      name,
      node.indexes,
    );
    return [root, ...children(node.children, root)];
  });
}
