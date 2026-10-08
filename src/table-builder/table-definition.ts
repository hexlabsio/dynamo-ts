import {
  LocalSecondaryIndexProperties,
  TableProperties,
} from '../cloudformation/index.js';
import {
  CdkDynamoModule,
  CdkTableProps,
  SstDynamoArgs,
  TerraformDynamoTable,
  TerraformExtraArguments,
  toHcl,
} from './infrastructure.js';
import { TableClient } from '../table-client.js';
import { DynamoConfig } from '../types/index.js';
import {
  IndexKeys,
  SchemaParts,
  SingleTableHelpers,
  SingleTableSchema,
  TablePartClient,
  TablePartClients,
  TablePartInfo,
  ValidateSchema,
  schemaParts,
  singleTableHelpers,
} from './single-table-builder.js';

export type SimpleDynamoType =
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

type ValidKeyTypes = string | number | Buffer;

export type ValidKeys<T> = (T extends Record<string, any>
  ? { [K in keyof T]: T[K] extends ValidKeyTypes ? K : never }[keyof T]
  : never) &
  keyof T;

/**
 * The DynamoDB type of a key attribute.
 */
export type KeyAttributeType = 'string' | 'number' | 'binary';

type DynamoAttributeType = 'S' | 'N' | 'B';

const dynamoAttributeTypes: Record<KeyAttributeType, DynamoAttributeType> = {
  string: 'S',
  number: 'N',
  binary: 'B',
};

/**
 * The key attribute type matching a TypeScript type, or never if it can't be a key.
 */
export type KeyAttributeTypeOf<V> = [NonNullable<V>] extends [string]
  ? 'string'
  : [NonNullable<V>] extends [number]
  ? 'number'
  : [NonNullable<V>] extends [Uint8Array]
  ? 'binary'
  : never;

/**
 * String keys don't need a type. Number and binary keys must say so, because types aren't available at runtime. Any
 * other type, including a mix such as string | number, can't be a key.
 */
export type KeyTypeArgument<V> = [KeyAttributeTypeOf<V>] extends [never]
  ? [type: never]
  : KeyAttributeTypeOf<V> extends 'string'
  ? [type?: 'string']
  : [type: KeyAttributeTypeOf<V>];

function attributeType(
  name: string,
  type: KeyAttributeType = 'string',
): Record<string, DynamoAttributeType> {
  return { [name]: dynamoAttributeTypes[type] };
}

class TableDefinitionBuilder<T> {
  withPartitionKey<const K extends ValidKeys<T>>(
    partitionKey: K,
    ...[type]: KeyTypeArgument<T[K]>
  ): TableDefinition<T, { partitionKey: K }> {
    return new TableDefinition<T, { partitionKey: K }>(
      { partitionKey },
      {},
      attributeType(partitionKey as string, type),
    );
  }
}

class IndexDefinitionBuilder<
  T,
  const KEYS extends DynamoTableKeyConfig<T>,
  const INDEXES extends Record<
    string,
    { global: boolean } & DynamoTableKeyConfig<T>
  >,
  const K extends keyof INDEXES,
> {
  constructor(
    private readonly tableDefinition: TableDefinition<T, KEYS, INDEXES>,
    private readonly index: K,
  ) {}

  withNoSortKey(): TableDefinition<T, KEYS, INDEXES> {
    return this.tableDefinition;
  }

  withSortKey<const SK extends keyof T>(
    sortKey: SK,
    ...[type]: KeyTypeArgument<T[SK]>
  ): TableDefinition<
    T,
    KEYS,
    INDEXES & { [KK in K]: INDEXES[K] & { sortKey: SK } }
  > {
    return new TableDefinition(
      this.tableDefinition.keyNames,
      {
        ...this.tableDefinition.indexes,
        [this.index]: { ...this.tableDefinition.indexes[this.index], sortKey },
      },
      {
        ...this.tableDefinition.attributeTypes,
        ...attributeType(sortKey as string, type),
      },
    );
  }
}

type PartitionAndSort<T, KEYS extends DynamoTableKeyConfig<T>> = KEYS extends {
  sortKey: infer S;
  partitionKey: infer K;
}
  ? (K | S) & keyof T
  : KEYS['partitionKey'] & keyof T;

export class TableDefinition<
  T = any,
  const KEYS extends DynamoTableKeyConfig<T> = any,
  const INDEXES extends Record<
    string,
    { global: boolean } & DynamoTableKeyConfig<T>
  > = {},
> {
  type: T = undefined as unknown as T;
  keys: Pick<T, PartitionAndSort<T, KEYS>> = undefined as unknown as any;
  withoutKeys: Omit<T, PartitionAndSort<T, KEYS>> = undefined as unknown as any;

  constructor(
    public readonly keyNames: KEYS,
    public readonly indexes: INDEXES,
    /** DynamoDB types of key attributes, string if not listed */
    public readonly attributeTypes: Record<string, DynamoAttributeType> = {},
  ) {}

  asIndex<I extends keyof INDEXES>(index: I): TableDefinition<T, INDEXES[I]> {
    return new TableDefinition<T, INDEXES[I]>(
      this.indexes[index],
      {},
      this.attributeTypes,
    );
  }

  withSortKey<const K extends Exclude<ValidKeys<T>, KEYS['partitionKey']>>(
    sortKey: K,
    ...[type]: KeyTypeArgument<T[K]>
  ): TableDefinition<
    T,
    { partitionKey: KEYS['partitionKey']; sortKey: K },
    INDEXES
  > {
    return new TableDefinition<
      T,
      { partitionKey: KEYS['partitionKey']; sortKey: K },
      INDEXES
    >({ ...this.keyNames, sortKey }, this.indexes, {
      ...this.attributeTypes,
      ...attributeType(sortKey as string, type),
    });
  }

  withGlobalSecondaryIndex<K extends string, PK extends keyof T>(
    name: K,
    partitionKey: PK,
    ...[type]: KeyTypeArgument<T[PK]>
  ): IndexDefinitionBuilder<
    T,
    KEYS,
    INDEXES & { [KK in K]: { partitionKey: PK; global: true } },
    K
  > {
    return new IndexDefinitionBuilder<
      T,
      KEYS,
      INDEXES & { [KK in K]: { partitionKey: PK; global: true } },
      K
    >(
      new TableDefinition(
        this.keyNames,
        {
          ...this.indexes,
          [name]: { global: true, partitionKey },
        },
        {
          ...this.attributeTypes,
          ...attributeType(partitionKey as string, type),
        },
      ),
      name,
    );
  }

  /**
   * Adds a local secondary index: the same partition key as the table, sorted by a different attribute. Local indexes
   * can only be created with the table, and limit each partition key value to 10 GB across the table and its local
   * indexes, but support strongly consistent reads.
   */
  withLocalSecondaryIndex<K extends string>(
    this: TableDefinition<T, KEYS & { sortKey: keyof T }, INDEXES>,
    name: K,
  ): {
    withSortKey<const SK extends keyof T>(
      sortKey: SK,
      ...[type]: KeyTypeArgument<T[SK]>
    ): TableDefinition<
      T,
      KEYS,
      INDEXES & {
        [KK in K]: {
          partitionKey: KEYS['partitionKey'];
          sortKey: SK;
          global: false;
        };
      }
    >;
  } {
    if (!this.keyNames.sortKey)
      throw new Error(
        `Local secondary index ${name} needs a table with a sort key`,
      );
    const builder = new IndexDefinitionBuilder(
      new TableDefinition(
        this.keyNames as any,
        {
          ...this.indexes,
          [name]: { global: false, partitionKey: this.keyNames.partitionKey },
        } as any,
        this.attributeTypes,
      ),
      name,
    );
    // Local indexes always have a sort key, so withNoSortKey isn't offered
    return {
      withSortKey: (sortKey: any, type?: any) =>
        (builder.withSortKey as any)(sortKey, type),
    } as any;
  }

  static ofType<T>(): TableDefinitionBuilder<T> {
    return new TableDefinitionBuilder();
  }

  /**
   * Defines a table for single table design as a tree of parts. Each property name is the name of a part and the
   * attribute used as its key.
   *
   * ```ts
   * TableDefinition.singleTable(({ part, join, child }) => ({
   *   customer: part<Customer>()
   *     .partitionedBy('store')
   *     .index('byName', { partition: [], sort: ['name'] })
   *     .with({
   *       address: join<Address>(),
   *       order: child<Order>().with({ line: join<OrderLine>() }),
   *     }),
   * }));
   * ```
   *
   * By default the table has a string partition key called **partition** and a string sort key called **sort**, and
   * each index uses attributes called **<index>_partition** and **<index>_sort**. Pass options first to change these.
   * Call **client** on the definition to get a client for each part.
   */
  static singleTable<S extends SingleTableSchema>(
    define: (helpers: SingleTableHelpers) => S & ValidateSchema<S>,
  ): SingleTableDefinition<'partition', 'sort', SchemaParts<S>>;
  static singleTable<
    const O extends SingleTableOptions,
    S extends SingleTableSchema,
  >(
    options: O,
    define: (helpers: SingleTableHelpers) => S & ValidateSchema<S>,
  ): SingleTableDefinition<
    O extends { partitionKey: infer PK extends string } ? PK : 'partition',
    O extends { sortKey: infer SK extends string } ? SK : 'sort',
    SchemaParts<S>
  >;
  static singleTable(
    ...args:
      | [(helpers: SingleTableHelpers) => SingleTableSchema]
      | [SingleTableOptions, (helpers: SingleTableHelpers) => SingleTableSchema]
  ): any {
    const [options, define] = args.length === 1 ? [{}, args[0]] : args;
    return new SingleTableDefinition(
      {
        partitionKey: options.partitionKey ?? 'partition',
        sortKey: options.sortKey ?? 'sort',
      },
      schemaParts(define(singleTableHelpers)),
      options.indexes,
    );
  }

  private attributeTypeOf(name: string): DynamoAttributeType {
    return this.attributeTypes[name] ?? 'S';
  }

  private indexKeysNames(): string[] {
    return Object.keys(this.indexes ?? {}).flatMap((key) => [
      this.indexes[key].partitionKey as string,
      ...(this.indexes[key].sortKey
        ? [this.indexes[key].sortKey! as string]
        : []),
    ]);
  }
  private allKeyNames(): string[] {
    return [
      ...new Set([
        this.keyNames.partitionKey as string,
        ...(this.keyNames.sortKey ? [this.keyNames.sortKey! as string] : []),
        ...this.indexKeysNames(),
      ]),
    ];
  }

  private indexDefinition(
    name: string,
    provisionedThroughput: TableProperties['ProvisionedThroughput'],
  ): LocalSecondaryIndexProperties {
    const index = this.indexes[name];
    return {
      IndexName: name,
      ...(index.global && provisionedThroughput
        ? { ProvisionedThroughput: provisionedThroughput }
        : {}),
      KeySchema: [
        {
          KeyType: 'HASH',
          AttributeName: index.partitionKey as string,
        },
        ...(index.sortKey
          ? [
              {
                KeyType: 'RANGE',
                AttributeName: index.sortKey as string,
              },
            ]
          : []),
      ],
      Projection: { ProjectionType: 'ALL' },
    };
  }

  asCloudFormation(
    name: string,
    properties: Omit<
      TableProperties,
      | 'KeySchema'
      | 'AttributeDefinitions'
      | 'GlobalSecondaryIndexes'
      | 'LocalSecondaryIndexes'
    > = {},
  ): TableProperties {
    const keys = this.allKeyNames();
    const indexNames = Object.keys(this.indexes);
    const globalIndexes = indexNames.filter((it) => this.indexes[it].global);
    const localIndexes = indexNames.filter((it) => !this.indexes[it].global);
    const globalConfig = globalIndexes.length
      ? {
          GlobalSecondaryIndexes: globalIndexes.map((name) =>
            this.indexDefinition(name, properties.ProvisionedThroughput),
          ),
        }
      : {};
    const localConfig = localIndexes.length
      ? {
          LocalSecondaryIndexes: localIndexes.map((name) =>
            this.indexDefinition(name, properties.ProvisionedThroughput),
          ),
        }
      : {};
    return {
      ...properties,
      ...(name ? { TableName: name } : {}),
      KeySchema: [
        {
          KeyType: 'HASH',
          AttributeName: this.keyNames.partitionKey as string,
        },
        ...(this.keyNames.sortKey
          ? [
              {
                KeyType: 'RANGE',
                AttributeName: this.keyNames.sortKey as string,
              },
            ]
          : []),
      ],
      AttributeDefinitions: keys.map((key) => ({
        AttributeName: key as string,
        AttributeType: this.attributeTypeOf(key),
      })),
      ...localConfig,
      ...globalConfig,
    };
  }

  private indexList(): {
    name: string;
    global: boolean;
    partitionKey: string;
    sortKey?: string;
  }[] {
    return Object.keys(this.indexes ?? {}).map((name) => ({
      name,
      global: this.indexes[name].global,
      partitionKey: this.indexes[name].partitionKey as string,
      sortKey: this.indexes[name].sortKey as string | undefined,
    }));
  }

  private globalIndexes() {
    return this.indexList().filter((it) => it.global);
  }

  private localIndexes(): { name: string; sortKey: string }[] {
    return this.indexList()
      .filter((it) => !it.global)
      .map((it) => {
        if (!it.sortKey)
          throw new Error(`Local secondary index ${it.name} needs a sort key`);
        return { name: it.name, sortKey: it.sortKey };
      });
  }

  /**
   * Attributes used as keys. A local index always shares the table's partition key, so only its sort key is included.
   */
  private usedKeyNames(): string[] {
    return [
      ...new Set([
        this.keyNames.partitionKey as string,
        ...(this.keyNames.sortKey ? [this.keyNames.sortKey as string] : []),
        ...this.globalIndexes().flatMap((it) => [
          it.partitionKey,
          ...(it.sortKey ? [it.sortKey] : []),
        ]),
        ...this.localIndexes().map((it) => it.sortKey),
      ]),
    ];
  }

  /**
   * Arguments for a Terraform `aws_dynamodb_table` resource. Use with CDKTF or in a `.tf.json` file, or call
   * **asTerraformHcl** for HCL.
   *
   * Extra arguments (billing_mode, tags, ttl and so on) are passed through. When read_capacity and write_capacity are
   * set they are copied to each global index, as required by PROVISIONED billing.
   */
  asTerraform<
    // eslint-disable-next-line @typescript-eslint/ban-types
    Props extends Record<string, unknown> = {},
  >(
    name: string,
    properties: TerraformExtraArguments<Props> = {} as any,
  ): TerraformDynamoTable & Props {
    const { read_capacity, write_capacity } = properties;
    const globalIndexes = this.globalIndexes();
    const localIndexes = this.localIndexes();
    return {
      name,
      ...properties,
      hash_key: this.keyNames.partitionKey as string,
      ...(this.keyNames.sortKey
        ? { range_key: this.keyNames.sortKey as string }
        : {}),
      attribute: this.usedKeyNames().map((key) => ({
        name: key,
        type: this.attributeTypeOf(key),
      })),
      ...(globalIndexes.length
        ? {
            global_secondary_index: globalIndexes.map((index) => ({
              name: index.name,
              key_schema: [
                { attribute_name: index.partitionKey, key_type: 'HASH' },
                ...(index.sortKey
                  ? [{ attribute_name: index.sortKey, key_type: 'RANGE' }]
                  : []),
              ],
              projection_type: 'ALL',
              ...(read_capacity !== undefined ? { read_capacity } : {}),
              ...(write_capacity !== undefined ? { write_capacity } : {}),
            })),
          }
        : {}),
      ...(localIndexes.length
        ? {
            local_secondary_index: localIndexes.map((index) => ({
              name: index.name,
              range_key: index.sortKey,
              projection_type: 'ALL',
            })),
          }
        : {}),
    } as any;
  }

  /**
   * A Terraform `aws_dynamodb_table` resource written in HCL, ready to save to a `.tf` file.
   *
   * @param resourceName - The Terraform resource name, e.g. `aws_dynamodb_table.<resourceName>`
   * @param name - The DynamoDB table name
   * @param properties - Extra arguments, as for **asTerraform**
   */
  asTerraformHcl<
    // eslint-disable-next-line @typescript-eslint/ban-types
    Props extends Record<string, unknown> = {},
  >(
    resourceName: string,
    name: string,
    properties: TerraformExtraArguments<Props> = {} as any,
  ): string {
    return toHcl(
      'aws_dynamodb_table',
      resourceName,
      this.asTerraform(name, properties),
    );
  }

  /**
   * Props for the CDK `TableV2` construct. Pass in the `aws-cdk-lib/aws-dynamodb` module so the real enums are used.
   *
   * ```ts
   * import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
   * new dynamodb.TableV2(this, 'Table', definition.asCdk(dynamodb, 'my-table', { billing: dynamodb.Billing.onDemand() }));
   * ```
   *
   * @param name - The table name, or undefined to let CloudFormation generate one
   * @param properties - Extra TableV2 props, passed through
   */
  asCdk<
    A,
    P,
    // eslint-disable-next-line @typescript-eslint/ban-types
    Props extends Record<string, unknown> = {},
  >(
    dynamodb: CdkDynamoModule<A, P>,
    name?: string,
    properties: Props & {
      [K in keyof CdkTableProps<A, P>]?: never;
    } = {} as any,
  ): CdkTableProps<A, P> & Props {
    const attribute = (key: string) => ({
      name: key,
      type: {
        S: dynamodb.AttributeType.STRING,
        N: dynamodb.AttributeType.NUMBER,
        B: dynamodb.AttributeType.BINARY,
      }[this.attributeTypeOf(key)],
    });
    const globalIndexes = this.globalIndexes();
    const localIndexes = this.localIndexes();
    return {
      ...properties,
      ...(name ? { tableName: name } : {}),
      partitionKey: attribute(this.keyNames.partitionKey as string),
      ...(this.keyNames.sortKey
        ? { sortKey: attribute(this.keyNames.sortKey as string) }
        : {}),
      ...(globalIndexes.length
        ? {
            globalSecondaryIndexes: globalIndexes.map((index) => ({
              indexName: index.name,
              partitionKey: attribute(index.partitionKey),
              ...(index.sortKey ? { sortKey: attribute(index.sortKey) } : {}),
              projectionType: dynamodb.ProjectionType.ALL,
            })),
          }
        : {}),
      ...(localIndexes.length
        ? {
            localSecondaryIndexes: localIndexes.map((index) => ({
              indexName: index.name,
              sortKey: attribute(index.sortKey),
              projectionType: dynamodb.ProjectionType.ALL,
            })),
          }
        : {}),
    } as any;
  }

  /**
   * Args for the SST v3 `sst.aws.Dynamo` component.
   *
   * ```ts
   * new sst.aws.Dynamo('MyTable', definition.asSst({ stream: 'new-and-old-images' }));
   * ```
   *
   * @param properties - Extra Dynamo args (stream, ttl, transform and so on), passed through
   */
  asSst<
    // eslint-disable-next-line @typescript-eslint/ban-types
    Props extends Record<string, unknown> = {},
  >(
    properties: Props & { [K in keyof SstDynamoArgs]?: never } = {} as any,
  ): SstDynamoArgs & Props {
    const globalIndexes = this.globalIndexes();
    const localIndexes = this.localIndexes();
    return {
      ...properties,
      fields: Object.fromEntries(
        this.usedKeyNames().map((key) => [
          key,
          ({ S: 'string', N: 'number', B: 'binary' } as const)[
            this.attributeTypeOf(key)
          ],
        ]),
      ),
      primaryIndex: {
        hashKey: this.keyNames.partitionKey as string,
        ...(this.keyNames.sortKey
          ? { rangeKey: this.keyNames.sortKey as string }
          : {}),
      },
      ...(globalIndexes.length
        ? {
            globalIndexes: Object.fromEntries(
              globalIndexes.map((index) => [
                index.name,
                {
                  hashKey: index.partitionKey,
                  ...(index.sortKey ? { rangeKey: index.sortKey } : {}),
                  projection: 'all',
                },
              ]),
            ),
          }
        : {}),
      ...(localIndexes.length
        ? {
            localIndexes: Object.fromEntries(
              localIndexes.map((index) => [
                index.name,
                { rangeKey: index.sortKey, projection: 'all' },
              ]),
            ),
          }
        : {}),
    } as any;
  }
}

export type DynamoTableKeyConfig<T> = {
  partitionKey: ValidKeys<T>;
  sortKey?: ValidKeys<T>;
};

type SingleTableType<PK extends string, SK extends string> = {
  [K in PK | SK]: string;
};

// Intersecting with ValidKeys satisfies TableDefinition's constraint while PK and SK are generic
type SingleTableKeys<PK extends string, SK extends string> = {
  partitionKey: PK & ValidKeys<SingleTableType<PK, SK>>;
  sortKey: SK & ValidKeys<SingleTableType<PK, SK>>;
};

/**
 * Options for **TableDefinition.singleTable**.
 */
export type SingleTableOptions = {
  /** The table's partition key attribute. Defaults to **partition**. */
  partitionKey?: string;
  /** The table's sort key attribute. Defaults to **sort**. */
  sortKey?: string;
  /**
   * Attribute names for indexes. Indexes not listed use **<index>_partition** and **<index>_sort**. Local indexes only
   * have a sort key attribute, as they use the table's partition key.
   */
  indexes?: Record<string, { partitionKey?: string; sortKey?: string }>;
};

const MAX_LOCAL_INDEXES = 5;

type SingleTableIndex = {
  global: boolean;
  partitionKey: string;
  sortKey?: string;
};

/**
 * Works out the table's secondary indexes from the indexes its parts use.
 */
function singleTableIndexes(
  keyNames: { partitionKey: string; sortKey: string },
  parts: TablePartInfo<any, any, any, any, any>[],
  configured: SingleTableOptions['indexes'] = {},
): Record<string, SingleTableIndex> {
  const usage: Record<
    string,
    { parts: string[]; sorted: boolean[]; local: boolean[] }
  > = {};
  parts.forEach((part) =>
    Object.entries(part.indexes as Record<string, IndexKeys>).forEach(
      ([name, keys]) => {
        usage[name] = usage[name] ?? { parts: [], sorted: [], local: [] };
        usage[name].parts.push(part.prefix);
        usage[name].sorted.push(keys.sort.length > 0);
        usage[name].local.push(keys.local);
      },
    ),
  );
  Object.keys(configured).forEach((name) => {
    if (!usage[name])
      throw new Error(`Index ${name} is configured but no part uses it`);
  });
  const indexes: Record<string, SingleTableIndex> = Object.fromEntries(
    Object.entries(usage).map(([name, { parts: users, sorted, local }]) => {
      const check = (values: boolean[], message: string) => {
        if (values.some((it) => it !== values[0]))
          throw new Error(
            `Every part using index ${name} must ${message}, check parts ${users.join(
              ', ',
            )}`,
          );
      };
      check(local, 'agree on whether it is local or global');
      check(sorted, 'either have sort keys or not');
      if (local[0]) {
        const attributes = configured[name] ?? { sortKey: `${name}_sort` };
        if (attributes.partitionKey)
          throw new Error(
            `Index ${name} is local so it uses the table's partition key, only configure its sortKey`,
          );
        if (!attributes.sortKey)
          throw new Error(`Index ${name} needs a sort key attribute name`);
        return [
          name,
          {
            global: false,
            partitionKey: keyNames.partitionKey,
            sortKey: attributes.sortKey,
          },
        ];
      }
      const hasSort = sorted[0];
      const attributes = configured[name] ?? {
        partitionKey: `${name}_partition`,
        ...(hasSort ? { sortKey: `${name}_sort` } : {}),
      };
      if (!attributes.partitionKey)
        throw new Error(`Index ${name} needs a partition key attribute name`);
      if (hasSort && !attributes.sortKey)
        throw new Error(`Index ${name} needs a sort key attribute name`);
      if (!hasSort && attributes.sortKey)
        throw new Error(
          `Index ${name} has a sort key attribute name but its parts have no sort keys`,
        );
      return [
        name,
        {
          global: true,
          partitionKey: attributes.partitionKey,
          ...(attributes.sortKey ? { sortKey: attributes.sortKey } : {}),
        },
      ];
    }),
  );
  const localCount = Object.values(indexes).filter((it) => !it.global).length;
  if (localCount > MAX_LOCAL_INDEXES)
    throw new Error(
      `A table can have at most ${MAX_LOCAL_INDEXES} local indexes, found ${localCount}`,
    );
  // Local indexes use the table's partition key, so only their sort key is a new attribute
  const attributes = [
    keyNames.partitionKey,
    keyNames.sortKey,
    ...Object.values(indexes).flatMap((it) => [
      ...(it.global ? [it.partitionKey] : []),
      ...(it.sortKey ? [it.sortKey] : []),
    ]),
  ];
  const duplicate = attributes.find(
    (name, index) => attributes.indexOf(name) !== index,
  );
  if (duplicate)
    throw new Error(
      `Single table key attributes must be unique, found ${duplicate} more than once`,
    );
  return indexes;
}

// Referring to the base class keeps the compiler from comparing generic single table definitions member by member
type SingleTableBase<PK extends string, SK extends string> = TableDefinition<
  SingleTableType<PK, SK>,
  SingleTableKeys<PK, SK>
>;

/**
 * A table definition for single table design, created with **TableDefinition.singleTable**.
 *
 * It is a normal **TableDefinition**, so it can be used anywhere one is expected (CloudFormation, CDK, Terraform, SST,
 * jest setup or a raw **TableClient**). Its secondary indexes come from the indexes its parts use. Call
 * **client** to get a client for each part.
 */
export class SingleTableDefinition<
  PK extends string,
  SK extends string,
  Parts extends TablePartInfo<any, any, any, any, any>,
> extends TableDefinition<SingleTableType<PK, SK>, SingleTableKeys<PK, SK>> {
  constructor(
    keyNames: { partitionKey: PK; sortKey: SK },
    public readonly parts: Parts[],
    indexes: SingleTableOptions['indexes'] = {},
  ) {
    super(keyNames as any, singleTableIndexes(keyNames, parts, indexes) as any);
    const names = parts.map((part) => part.prefix);
    const duplicate = names.find(
      (name, index) => names.indexOf(name) !== index,
    );
    if (duplicate) {
      throw new Error(
        `Single table parts must have unique names, found ${duplicate} more than once`,
      );
    }
  }

  /**
   * Builds a client for each part, keyed by part name.
   */
  client(
    config: DynamoConfig,
  ): TablePartClients<Parts, SingleTableBase<PK, SK>> {
    const base: SingleTableBase<PK, SK> = this;
    const tableClient = new TableClient(base, config);
    const parts: TablePartInfo<any, any, any, any, any>[] = this.parts;
    return parts.reduce(
      (prev, next) => ({
        ...prev,
        [next.prefix]: new TablePartClient(
          next.part,
          next,
          next.prefix,
          tableClient as any,
        ),
      }),
      {},
    ) as any;
  }
}
