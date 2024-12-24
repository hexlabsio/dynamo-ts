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

  private intoParentage(
    items: any[],
    chain: string[],
    search: (item: any) => boolean = () => true,
  ): any {
    if (chain.length === 1) {
      const last = chain[0];
      return items
        .filter((it) => {
          return (
            it[this.tableClient.tableConfig.keyNames.sortKey].startsWith(
              `#${last.toUpperCase()}`,
            ) && search(it)
          );
        })
        .map((it) => {
          return it;
        });
    }
    const name = chain[0];
    const results = items.filter(
      (it) =>
        it[this.tableClient.tableConfig.keyNames.sortKey].startsWith(
          `#${name.toUpperCase()}`,
        ) && search(it),
    );
    return results.map((it) => {
      return {
        item: it,
        member: this.intoParentage(items, chain.slice(1), (o) => {
          return search(o) && o[name] === it[name];
        }),
      };
    });
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

  async queryWithParents<PROJECTION = null>(
    partition: { [K in T['partitions'][number]]: string },
    options: QuerierInput<CombinedTypes<Info>, PROJECTION> = {},
  ): Promise<QuerierReturn<ParentTypes<ParentType<Info>>, PROJECTION>> {
    const partitionString = this.part.partitions.reduce(
      (prev, next) =>
        `${prev}#${next.toString().toUpperCase()}$${partition[next]}`,
      '',
    );
    const result = await this.tableClient.query(
      {
        [this.tableClient.tableConfig.keyNames.partitionKey]: partitionString,
      } as any,
      options as any,
    );
    const chain = this.getParentChain().map((it) => it.prefix);
    const member = this.intoParentage(result.member, chain);
    return { ...result, member } as any;
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
