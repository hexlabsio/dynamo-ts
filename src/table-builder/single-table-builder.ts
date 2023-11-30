import { GetItemOptions, GetItemReturn } from '../dynamo-getter';
import { PutItemOptions, PutItemReturn, PutReturnValues } from '../dynamo-puter';
import { QuerierInput, QuerierReturn } from '../dynamo-querier';
import { TableClient } from '../table-client';
import { DynamoConfig } from '../types';
import { TableDefinition, ValidKeys } from './table-definition';

type TablePart<T> = {
  partitions: (keyof T)[];
  sorts: (keyof T)[];
}

type SortKeys<KEYS extends (string | number | symbol)[], O extends (string | number | symbol)[] = KEYS> =
  KEYS extends [infer A extends string]
    ? { [K in A]:  (((key: string) => any)) } & { [K in O[number]]?: string }
    : KEYS extends [infer A extends string, ...infer TAIL extends string[]]
      ? { [K in A]: (((key: string) => SortKeys<TAIL, O>)) } & { [K in O[number]]?: string }
      : never;

export type TablePartClients<T> =
  T extends [TablePartInfo<infer A, infer B, infer C, infer P>]
  ? { [K in C]: TablePartClient<A,B, TablePartInfo<A,B, C, P>> }
  : T extends [TablePartInfo<infer A, infer B, infer C, infer P>, ...infer TAIL]
    ? { [K in C]: TablePartClient<A,B, TablePartInfo<A,B, C, P>> } & TablePartClients<TAIL>
    : never;

export type TablePartsType<T> =
  T extends [TablePartInfo<infer A, infer B, infer C>]
    ? A & { partition: string, sort: string }
    : T extends [TablePartInfo<infer A, infer B, infer C>, ...infer TAIL]
      ? A & TablePartsType<TAIL>
      : never;

export type ParentTypes<T extends any[]> = T extends [infer A] ? A : T extends [infer A, ...infer Rest] ? { item: A, member: ParentTypes<Rest>[] } : never;
export type ParentType<P extends TablePartInfo<any, any, any, any> | null, Depth extends 0[] = []> = Depth['length'] extends 5 ? [] :
  P extends TablePartInfo<infer A, any, any, infer PP> ? (PP extends null ? [A] : [...ParentType<PP, [...Depth, 0]>, A]) : [];

export type CombinedTypes<P extends TablePartInfo<any, any, any, any> | null, Depth extends 0[] = []> = Depth['length'] extends 5 ? {} :
  P extends TablePartInfo<infer A, any, any, infer PP> ? (PP extends null ? A : A & CombinedTypes<PP, [...Depth, 0]>) : {};

export class TablePartClient<TableType, T extends TablePart<TableType>, Info extends TablePartInfo<any, any, any, any>> {
  constructor(
    private readonly part: T,
    private readonly parent: Info,
    private readonly prefix: string,
    private readonly tableClient: TableClient<TableDefinition<{ partition: string, sort: string}, {partitionKey: 'partition', sortKey: 'sort'}>>
  ) {}
  private proxySetter(set: (name: string, value: string) => void) {
    const self = this;
    return new Proxy({}, {
      get(target, name) {
        return (value: string) => {
          set(name as any, value);
          return self.proxySetter(set);
        }
      }
    });
  }

  getParentChain(info: TablePartInfo<any,any, any, any>= this.parent): TablePartInfo<any,any, any, any>[] {
    if(info.parents) return [...this.getParentChain(info.parents), info];
    return [info];
  }

  private intoParentage(items: any[], chain: string[], search: (item: any) => boolean = () => true): any {
    if(chain.length === 1) {
      const last = chain[0];
      return items.filter(it => it.sort.startsWith(`#${last.toUpperCase()}`) && search(it)).map(({partition, sort, ...rest}) => rest)
    }
    const name = chain[0];
    const results = items.filter(it => it.sort.startsWith(`#${name.toUpperCase()}`) && search(it));
    return results.map(({partition, sort, ...item}) => ({ item, member: this.intoParentage(items, chain.slice(1), o => {return search(o) && o[name] === item[name]} )}))
  }

  async query<PROJECTION = null>(
    partition: { [K in T['partitions'][number]]: string },
    keys: (keys: SortKeys<T['sorts']>) => { [K in T['sorts'][number]]?: string } = () => { return {} },
    options: QuerierInput<TableType, PROJECTION> = {},
  ): Promise<QuerierReturn<TableType, PROJECTION>> {
    const keyResult: any = {}
    keys(this.proxySetter((name: string, value: string) => { keyResult[name] = value; }) as any);
    const partitionString = this.part.partitions.reduce((prev, next) => `${prev}#${next.toString().toUpperCase()}$${partition[next]}`, '');
    let sortString = this.part.sorts.reduce((prev, next) => keyResult[next] ? `${prev}#${next.toString().toUpperCase()}$${keyResult[next]}`: prev, '');
    if(this.prefix && !sortString.startsWith(`#${this.prefix.toUpperCase()}`)) {
      sortString = `#${this.prefix.toUpperCase()}${sortString}`
    }
    const result = await this.tableClient.query({partition: partitionString, sort: sortKey => sortKey.beginsWith(sortString)}, options as any);
    return {...result, member: result.member.map(({partition, sort, ...rest}) => rest) } as any;
  }

  async queryWithParents<PROJECTION = null>(
    partition: { [K in T['partitions'][number]]: string },
    options: QuerierInput<CombinedTypes<Info>, PROJECTION> = {},
  ): Promise<QuerierReturn<ParentTypes<ParentType<Info>>, PROJECTION>> {
    const partitionString = this.part.partitions.reduce((prev, next) => `${prev}#${next.toString().toUpperCase()}$${partition[next]}`, '');
    const result = await this.tableClient.query({partition: partitionString}, options as any);
    const chain = this.getParentChain().map(it => it.prefix);
    const member = this.intoParentage(result.member, chain);
    return {...result, member } as any;
  }

  async put<RETURN extends PutReturnValues = 'NONE'>(item: TableType, options: PutItemOptions<TableType, RETURN> = {}): Promise<PutItemReturn<TableType, RETURN>> {
    const partition = this.part.partitions.reduce((prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`, '');
    let sort = this.part.sorts.reduce((prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`, '');
    if(this.prefix && !sort.startsWith(`#${this.prefix.toUpperCase()}`)) {
      sort = `#${this.prefix.toUpperCase()}${sort}`
    }
    return await this.tableClient.put({...item, partition, sort}, options as any) as any;
  }

  async get<PROJECTION = null>(item: { [K in T['partitions'][number] | T['sorts'][number]]: string }, options: GetItemOptions<TableType, PROJECTION> = {}): Promise<GetItemReturn<TableType, PROJECTION>> {
    const partition = this.part.partitions.reduce((prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`, '');
    let sort = this.part.sorts.reduce((prev, next) => `${prev}#${next.toString().toUpperCase()}$${item[next]}`, '');
    if(this.prefix && !sort.startsWith(`#${this.prefix.toUpperCase()}`)) {
      sort = `#${this.prefix.toUpperCase()}${sort}`
    }
    return await this.tableClient.get({...item, partition, sort}, options as any) as any;
  }

  static fromParts<T extends TablePartInfo<any, any, any, any>[]>(config: DynamoConfig, ...parts: T): TablePartClients<T>
  {
    return parts.reduce((prev, next) => ({
      ...prev, [next.prefix]: new TablePartClient(next.part, next, next.prefix, new TableClient(TableDefinition.ofType<any>().withPartitionKey('partition').withSortKey('sort'), config) as any)
    }), {}) as any;
  }

  rawTableClient(): TableDefinition<CombinedTypes<Info> & { partition: string; sort: string }> {
    return TableDefinition.ofType<{ partition: string; sort: string }>().withPartitionKey('partition').withSortKey('sort') as any;
  }

}

export class TablePartInfo<TableType, T extends TablePart<TableType>, NAME extends keyof any, Parent extends TablePartInfo<any, any, any, any> | null = null> {
  constructor(
    public readonly part: T,
    public readonly parents: Parent,
    public readonly prefix: string,
  ) {
  }

  joinPart<JoinTableType extends Pick<TableType, T['partitions'][number] | T['sorts'][number]>>(): {
    withKey<K extends ValidKeys<JoinTableType>>(key: K): TablePartInfo<JoinTableType, { partitions: T['partitions'], sorts: [...T['sorts'], K] }, K, TablePartInfo<TableType, T, NAME, Parent>>
  } {
    return { withKey: (key: string) => new TablePartInfo({partitions: this.part.partitions, sorts: [...this.part.sorts, key]} as any, this, key) } as any;
  }

  childPart<JoinTableType extends Pick<TableType, T['partitions'][number] | T['sorts'][number]>>(): {
    withKey<K extends ValidKeys<JoinTableType>>(key: K): TablePartInfo<JoinTableType, { partitions: [...T['partitions'], ...T['sorts']], sorts: [K] }, K>
  } {
    return { withKey: (key: string) => new TablePartInfo({partitions: [...this.part.partitions, ...this.part.sorts], sorts: [key]} as any, null, key) } as any;
  }

  static from<TableType>(): { withKeys<K extends ValidKeys<TableType>, K2 extends Exclude<ValidKeys<TableType>, K>>(partitionKey: K, sortKey: K2): TablePartInfo<TableType, { partitions: [K], sorts: [K2], parents: [] }, K2> } {
    return { withKeys: (partitionKey: string, sortKey: string) => new TablePartInfo({partitions: [partitionKey], sorts: [sortKey]} as any, null, sortKey) } as any;
  }
}

