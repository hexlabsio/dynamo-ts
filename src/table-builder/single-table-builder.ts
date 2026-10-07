import { ConsumedCapacity } from '@aws-sdk/client-dynamodb';
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
import { TableClient } from '../table-client.js';
import { DynamoConfig } from '../types/index.js';
import { TableDefinition, ValidKeys } from './table-definition.js';

type TablePart<T> = {
  partitions: (keyof T & string)[];
  sorts: (keyof T & string)[];
};

type SortKeys<
  KEYS extends (string | number | symbol)[],
  O extends (string | number | symbol)[] = KEYS,
> = KEYS extends [infer A extends string]
  ? { [K in A]: (key: string) => any } & { [K in O[number]]?: string }
  : KEYS extends [infer A extends string, ...infer TAIL extends string[]]
  ? { [K in A]: (key: string) => SortKeys<TAIL, O> } & {
      [K in O[number]]?: string;
    }
  : never;

export type TablePartClients<T, D extends TableDefinition> = T extends [
  TablePartInfo<infer A, infer B, infer C, infer P>,
]
  ? { [K in C]: TablePartClient<A, B, TablePartInfo<A, B, C, P>, D> }
  : T extends [TablePartInfo<infer A, infer B, infer C, infer P>, ...infer TAIL]
  ? {
      [K in C]: TablePartClient<A, B, TablePartInfo<A, B, C, P>, D>;
    } & TablePartClients<TAIL, D>
  : never;

export type ParentTypes<T extends any[]> = T extends [infer A]
  ? A
  : T extends [infer A, ...infer Rest]
  ? { item: A; member: ParentTypes<Rest>[] }
  : never;
export type ParentType<
  P extends TablePartInfo<any, any, any, any> | null,
  Depth extends 0[] = [],
> = Depth['length'] extends 5
  ? []
  : P extends TablePartInfo<infer A, any, any, infer PP>
  ? PP extends null
    ? [A]
    : [...ParentType<PP, [...Depth, 0]>, A]
  : [];

export type CombinedTypes<
  P extends TablePartInfo<any, any, any, any> | null,
  Depth extends 0[] = [],
> = Depth['length'] extends 5
  ? {}
  : P extends TablePartInfo<infer A, any, any, infer PP>
  ? PP extends null
    ? A
    : A & CombinedTypes<PP, [...Depth, 0]>
  : {};

export const defaultBaseTable = TableDefinition.ofType<{
  partition: string;
  sort: string;
}>()
  .withPartitionKey('partition')
  .withSortKey('sort');

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
  Info extends TablePartInfo<any, any, any, any>,
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
    info: TablePartInfo<any, any, any, any> = this.parent,
  ): TablePartInfo<any, any, any, any>[] {
    if (info.parents) return [...this.getParentChain(info.parents), info];
    return [info];
  }

  private keySegments(keys: string[], values: Record<string, any>): string {
    return keys.reduce(
      (prev, next) => `${prev}#${next.toUpperCase()}$${values[next]}`,
      '',
    );
  }

  /**
   * The sort key prefix shared by every item of a joined part that belongs to the given parent item.
   */
  private joinedPrefix(
    info: TablePartInfo<any, any, any, any>,
    parentInfo: TablePartInfo<any, any, any, any>,
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
    partition: { [K in T['partitions'][number]]: string },
    keys: (keys: SortKeys<T['sorts']>) => {
      [K in T['sorts'][number]]?: string;
    } = (() => {
      return {};
    }) as any,
    options: QuerierInput<TableType, PROJECTION> = {},
  ): Promise<QuerierReturn<TableType, PROJECTION>> {
    const keyResult: any = {};
    keys(
      this.proxySetter((name: string, value: string) => {
        keyResult[name] = value;
      }) as any,
    );
    const partitionString = this.part.partitions.reduce(
      (prev, next) =>
        `${prev}#${next.toString().toUpperCase()}$${partition[next]}`,
      '',
    );
    let sortString = this.part.sorts.reduce(
      (prev, next) =>
        keyResult[next]
          ? `${prev}#${next.toString().toUpperCase()}$${keyResult[next]}`
          : prev,
      '',
    );
    if (
      this.prefix &&
      !sortString.startsWith(`#${this.prefix.toUpperCase()}`)
    ) {
      sortString = `#${this.prefix.toUpperCase()}${sortString}`;
    }
    const result = await this.tableClient.query(
      {
        [this.tableClient.tableConfig.keyNames.partitionKey]: partitionString,
        [this.tableClient.tableConfig.keyNames.sortKey]: (sortKey: any) =>
          sortKey.beginsWith(sortString),
      } as any,
      options as any,
    );
    return result as any;
  }

  /**
   * Queries the top level parents in the partition along with all of their joined children, grouped into a tree.
   *
   * Paging applies to the top level parents: **limit** caps how many are read per page and **next** continues from
   * the previous page. Every page contains the complete set of children for the parents it returns.
   */
  async queryWithParents<PROJECTION = null>(
    partition: { [K in T['partitions'][number]]: string },
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
    partition: { [K in T['partitions'][number]]: string },
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
    const partition = this.part.partitions.reduce(
      (prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
      '',
    );
    let sort = this.part.sorts.reduce(
      (prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
      '',
    );
    if (this.prefix && !sort.startsWith(`#${this.prefix.toUpperCase()}`)) {
      sort = `#${this.prefix.toUpperCase()}${sort}`;
    }
    const keys: Definition['type'] = {
      [this.tableClient.tableConfig.keyNames.partitionKey]: partition,
      [this.tableClient.tableConfig.keyNames.sortKey]: sort,
    };
    const putResult = (await this.tableClient.put(
      { ...item, ...keys },
      options as any,
    )) as any;
    return { ...putResult, keys };
  }

  batchPut(
    items: TableType[],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    return this.tableClient.batchPut(
      items.map((item) => {
        const partition = this.part.partitions.reduce(
          (prev, next) =>
            `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
          '',
        );
        let sort = this.part.sorts.reduce(
          (prev, next) =>
            `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
          '',
        );
        if (this.prefix && !sort.startsWith(`#${this.prefix.toUpperCase()}`)) {
          sort = `#${this.prefix.toUpperCase()}${sort}`;
        }
        return {
          ...item,
          [this.tableClient.tableConfig.keyNames.partitionKey]: partition,
          [this.tableClient.tableConfig.keyNames.sortKey]: sort,
        };
      }),
      options as any,
    );
  }

  async get<PROJECTION = null>(
    item: { [K in T['partitions'][number] | T['sorts'][number]]: string },
    options: GetItemOptions<TableType, PROJECTION> = {},
  ): Promise<GetItemReturnSingleTable<Definition, TableType, PROJECTION>> {
    const partition = this.part.partitions.reduce(
      (prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
      '',
    );
    let sort = this.part.sorts.reduce(
      (prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
      '',
    );
    if (this.prefix && !sort.startsWith(`#${this.prefix.toUpperCase()}`)) {
      sort = `#${this.prefix.toUpperCase()}${sort}`;
    }
    const keys: Definition['type'] = {
      [this.tableClient.tableConfig.keyNames.partitionKey]: partition,
      [this.tableClient.tableConfig.keyNames.sortKey]: sort,
    };
    const result = (await this.tableClient.get(
      { ...keys },
      options as any,
    )) as any;
    return { ...result, keys };
  }

  async delete<RETURN extends DeleteReturnValues>(
    item: { [K in T['partitions'][number] | T['sorts'][number]]: string },
    options: DeleteItemOptions<TableType, RETURN> = {},
  ): Promise<DeleteItemReturnSingleTable<Definition, TableType, RETURN>> {
    const partition = this.part.partitions.reduce(
      (prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
      '',
    );
    let sort = this.part.sorts.reduce(
      (prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`,
      '',
    );
    if (this.prefix && !sort.startsWith(`#${this.prefix.toUpperCase()}`)) {
      sort = `#${this.prefix.toUpperCase()}${sort}`;
    }
    const keys: Definition['type'] = {
      [this.tableClient.tableConfig.keyNames.partitionKey]: partition,
      [this.tableClient.tableConfig.keyNames.sortKey]: sort,
    };
    const result = (await this.tableClient.delete(
      { ...keys },
      options as any,
    )) as any;
    return { ...result, keys };
  }

  static fromPartsWithBaseTable<
    Definition extends TableDefinition,
    T extends TablePartInfo<any, any, any, any>[],
  >(
    baseTable: Definition,
    config: DynamoConfig,
    ...parts: T
  ): TablePartClients<T, Definition> {
    return parts.reduce(
      (prev, next) => ({
        ...prev,
        [next.prefix]: new TablePartClient(
          next.part,
          next,
          next.prefix,
          new TableClient(baseTable, config) as any,
        ),
      }),
      {},
    ) as any;
  }

  static fromParts<T extends TablePartInfo<any, any, any, any>[]>(
    config: DynamoConfig,
    ...parts: T
  ): TablePartClients<T, typeof defaultBaseTable> {
    return TablePartClient.fromPartsWithBaseTable(
      defaultBaseTable,
      config,
      ...parts,
    );
  }
}

export class TablePartInfo<
  TableType,
  T extends TablePart<TableType>,
  NAME extends keyof any,
  Parent extends TablePartInfo<any, any, any, any> | null = null,
> {
  constructor(
    public readonly part: T,
    public readonly parents: Parent,
    public readonly prefix: string,
  ) {}

  joinPart<
    JoinTableType extends Pick<
      TableType,
      T['partitions'][number] | T['sorts'][number]
    >,
  >(): {
    withKey<K extends ValidKeys<JoinTableType>>(
      key: K,
    ): TablePartInfo<
      JoinTableType,
      { partitions: T['partitions']; sorts: [...T['sorts'], K] },
      K,
      TablePartInfo<TableType, T, NAME, Parent>
    >;
  } {
    return {
      withKey: (key: string) =>
        new TablePartInfo(
          {
            partitions: this.part.partitions,
            sorts: [...this.part.sorts, key],
          } as any,
          this,
          key,
        ),
    } as any;
  }

  childPart<
    JoinTableType extends Pick<
      TableType,
      T['partitions'][number] | T['sorts'][number]
    >,
  >(): {
    withKey<K extends ValidKeys<JoinTableType>>(
      key: K,
    ): TablePartInfo<
      JoinTableType,
      { partitions: [...T['partitions'], ...T['sorts']]; sorts: [K] },
      K
    >;
  } {
    return {
      withKey: (key: string) =>
        new TablePartInfo(
          {
            partitions: [...this.part.partitions, ...this.part.sorts],
            sorts: [key],
          } as any,
          null,
          key,
        ),
    } as any;
  }

  static from<TableType>(): {
    withKeys<
      K extends ValidKeys<TableType> & string,
      K2 extends Exclude<ValidKeys<TableType>, K> & string,
    >(
      partitionKey: K,
      sortKey: K2,
    ): TablePartInfo<
      TableType,
      { partitions: [K]; sorts: [K2]; parents: [] },
      K2
    >;
  } {
    return {
      withKeys: (partitionKey: string, sortKey: string) =>
        new TablePartInfo(
          { partitions: [partitionKey], sorts: [sortKey] } as any,
          null,
          sortKey,
        ),
    } as any;
  }
}
