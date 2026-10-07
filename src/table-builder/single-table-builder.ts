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
  ? { [K in A]: (key: string) => any } & { [K in O[number]]?: string }
  : KEYS extends [infer A extends string, ...infer TAIL extends string[]]
  ? { [K in A]: (key: string) => SortKeys<TAIL, O> } & {
      [K in O[number]]?: string;
    }
  : never;

export type TablePartClients<Parts, D extends TableDefinition> = {
  [P in Parts as P extends TablePartInfo<any, any, infer C, any>
    ? C & string
    : never]: P extends TablePartInfo<infer A, infer B, infer C, infer PP>
    ? TablePartClient<A, B, TablePartInfo<A, B, C, PP>, D>
    : never;
};

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
}

type AnyPart = TablePartInfo<any, any, any, any>;

// Extract keeps the key lists unchanged, while telling the compiler they are keys of the part's type
type KeysOf<J, X> = Extract<X, (keyof J & string)[]>;

export type RootPart<
  TableType,
  K extends string,
  K2 extends string,
> = TablePartInfo<
  TableType,
  { partitions: KeysOf<TableType, [K]>; sorts: KeysOf<TableType, [K2]> },
  K2
>;

export type JoinedPart<
  Parent extends AnyPart,
  J,
  K extends string,
> = Parent extends TablePartInfo<any, infer T, any, any>
  ? TablePartInfo<
      J,
      {
        partitions: KeysOf<J, T['partitions']>;
        sorts: KeysOf<J, [...T['sorts'], K]>;
      },
      K,
      Parent
    >
  : never;

export type ChildPart<
  Parent extends AnyPart,
  J,
  K extends string,
> = Parent extends TablePartInfo<any, infer T, any, any>
  ? TablePartInfo<
      J,
      {
        partitions: KeysOf<J, [...T['partitions'], ...T['sorts']]>;
        sorts: KeysOf<J, [K]>;
      },
      K
    >
  : never;

/*
 * Single table schema
 *
 * A single table is described as a tree of nodes, where each property name is the name of a part and the attribute
 * used as its key.
 */

export type SchemaChildren = Record<
  string,
  JoinNode<any, any> | ChildNode<any, any>
>;

export interface PartNode<TableType, PK extends string, Children> {
  readonly kind: 'part';
  readonly partitionKey: PK;
  readonly children: Children;
  /** Type only */
  readonly tableType?: TableType;
}

export interface JoinNode<TableType, Children> {
  readonly kind: 'join';
  readonly children: Children;
  /** Type only */
  readonly tableType?: TableType;
}

export interface ChildNode<TableType, Children> {
  readonly kind: 'child';
  readonly children: Children;
  /** Type only */
  readonly tableType?: TableType;
}

export type SingleTableSchema = Record<string, PartNode<any, any, any>>;

export type SingleTableHelpers = {
  /**
   * A root part. Its partition key is the attribute passed to **partitionedBy**, and its sort key is the property name.
   */
  part<TableType>(): {
    partitionedBy<const PK extends ValidKeys<TableType> & string>(
      partitionKey: PK,
    ): PartNode<TableType, PK, {}>;
    partitionedBy<
      const PK extends ValidKeys<TableType> & string,
      const Children extends SchemaChildren,
    >(
      partitionKey: PK,
      children: Children,
    ): PartNode<TableType, PK, Children>;
  };
  /**
   * A part that shares its parent's partition, so it can be read together with the parent. Use it for children that
   * are small or bounded.
   */
  join<TableType>(): JoinNode<TableType, {}> & {
    with<const Children extends SchemaChildren>(
      children: Children,
    ): JoinNode<TableType, Children>;
  };
  /**
   * A part that gets its own partition under its parent. Use it for children that can grow without bound.
   */
  child<TableType>(): ChildNode<TableType, {}> & {
    with<const Children extends SchemaChildren>(
      children: Children,
    ): ChildNode<TableType, Children>;
  };
};

export const singleTableHelpers: SingleTableHelpers = {
  part: () => ({
    partitionedBy: (partitionKey: string, children = {}) => ({
      kind: 'part',
      partitionKey,
      children,
    }),
  }),
  join: () => ({
    kind: 'join',
    children: {},
    with: (children: SchemaChildren) => ({ kind: 'join', children }),
  }),
  child: () => ({
    kind: 'child',
    children: {},
    with: (children: SchemaChildren) => ({ kind: 'child', children }),
  }),
} as any;

type SchemaError<Message extends string> = `Error: ${Message}`;

type ValidateChildren<Children, ParentType, ParentKeys extends string> = {
  [N in keyof Children]: Children[N] extends
    | JoinNode<infer T, infer C>
    | ChildNode<infer T, infer C>
    ? T extends Pick<ParentType, ParentKeys & keyof ParentType>
      ? N extends ValidKeys<T> & string
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
  [N in keyof S]: S[N] extends PartNode<infer T, infer PK, infer C>
    ? N extends ValidKeys<T> & string
      ? N extends PK
        ? SchemaError<`${N & string} can't be both the partition and sort key`>
        : S[N] & { children: ValidateChildren<C, T, PK | N> }
      : SchemaError<`${N &
          string} must be a string or number attribute of its part`>
    : SchemaError<`${N & string} must be created with part()`>;
};

type FlattenChildren<Children, Parent extends AnyPart> = {
  [N in keyof Children & string]: Children[N] extends JoinNode<infer T, infer C>
    ? JoinedPart<Parent, T, N> | FlattenChildren<C, JoinedPart<Parent, T, N>>
    : Children[N] extends ChildNode<infer T, infer C>
    ? ChildPart<Parent, T, N> | FlattenChildren<C, ChildPart<Parent, T, N>>
    : never;
}[keyof Children & string];

/**
 * Every part in the schema, as a union.
 */
export type SchemaParts<S> = {
  [N in keyof S & string]: S[N] extends PartNode<infer T, infer PK, infer C>
    ? RootPart<T, PK, N> | FlattenChildren<C, RootPart<T, PK, N>>
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
          ? new TablePartInfo<any, any, any, any>(
              {
                partitions: parent.part.partitions,
                sorts: [...parent.part.sorts, name],
              },
              parent,
              name,
            )
          : new TablePartInfo<any, any, any, any>(
              {
                partitions: [...parent.part.partitions, ...parent.part.sorts],
                sorts: [name],
              },
              null,
              name,
            );
      return [part, ...children(node.children, part)];
    });
  return Object.entries(schema).flatMap(([name, node]) => {
    const root = new TablePartInfo<any, any, any, any>(
      { partitions: [node.partitionKey], sorts: [name] },
      null,
      name,
    );
    return [root, ...children(node.children, root)];
  });
}
