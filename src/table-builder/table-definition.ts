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

class TableDefinitionBuilder<T> {
  withPartitionKey<const K extends ValidKeys<T>>(
    partitionKey: K,
  ): TableDefinition<T, { partitionKey: K }> {
    return new TableDefinition<T, { partitionKey: K }>({ partitionKey }, {});
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
  ): TableDefinition<
    T,
    KEYS,
    INDEXES & { [KK in K]: INDEXES[K] & { sortKey: SK } }
  > {
    return new TableDefinition(this.tableDefinition.keyNames, {
      ...this.tableDefinition.indexes,
      [this.index]: { ...this.tableDefinition.indexes[this.index], sortKey },
    });
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
  ) {}

  asIndex<I extends keyof INDEXES>(index: I): TableDefinition<T, INDEXES[I]> {
    return new TableDefinition<T, INDEXES[I]>(this.indexes[index], {});
  }

  withSortKey<const K extends Exclude<ValidKeys<T>, KEYS['partitionKey']>>(
    sortKey: K,
  ): TableDefinition<
    T,
    { partitionKey: KEYS['partitionKey']; sortKey: K },
    INDEXES
  > {
    return new TableDefinition<
      T,
      { partitionKey: KEYS['partitionKey']; sortKey: K },
      INDEXES
    >({ ...this.keyNames, sortKey }, this.indexes);
  }

  withGlobalSecondaryIndex<K extends string, PK extends keyof T>(
    name: K,
    partitionKey: PK,
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
      new TableDefinition(this.keyNames, {
        ...this.indexes,
        [name]: { global: true, partitionKey },
      }),
      name,
    );
  }

  withLocalSecondaryIndex<K extends string, PK extends keyof T>(
    name: K,
    partitionKey: PK,
  ): IndexDefinitionBuilder<
    T,
    KEYS,
    INDEXES & { [KK in K]: { partitionKey: PK; global: false } },
    K
  > {
    return new IndexDefinitionBuilder<
      T,
      KEYS,
      INDEXES & { [KK in K]: { partitionKey: PK; global: false } },
      K
    >(
      new TableDefinition(this.keyNames, {
        ...this.indexes,
        [name]: { global: false, partitionKey },
      }),
      name,
    );
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
   *   customer: part<Customer>().partitionedBy('store', {
   *     address: join<Address>(),
   *     order: child<Order>().with({ line: join<OrderLine>() }),
   *   }),
   * }));
   * ```
   *
   * By default the table has a string partition key called **partition** and a string sort key called **sort**. Call
   * **client** on the definition to get a client for each part.
   */
  static singleTable<S extends SingleTableSchema>(
    define: (helpers: SingleTableHelpers) => S & ValidateSchema<S>,
  ): SingleTableDefinition<'partition', 'sort', SchemaParts<S>>;
  static singleTable<
    const PK extends string,
    const SK extends string,
    S extends SingleTableSchema,
  >(
    partitionKey: PK,
    sortKey: Exclude<SK, PK>,
    define: (helpers: SingleTableHelpers) => S & ValidateSchema<S>,
  ): SingleTableDefinition<PK, SK, SchemaParts<S>>;
  static singleTable(
    ...args:
      | [(helpers: SingleTableHelpers) => SingleTableSchema]
      | [string, string, (helpers: SingleTableHelpers) => SingleTableSchema]
  ): any {
    const [partitionKey, sortKey, define] =
      args.length === 1 ? ['partition', 'sort', args[0]] : args;
    return new SingleTableDefinition(
      { partitionKey, sortKey },
      schemaParts(define(singleTableHelpers)),
    );
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
        AttributeType: 'S',
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
      attribute: this.usedKeyNames().map((key) => ({ name: key, type: 'S' })),
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
      type: dynamodb.AttributeType.STRING,
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
        this.usedKeyNames().map((key) => [key, 'string']),
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
 * A table definition for single table design, created with **TableDefinition.singleTable**.
 *
 * It is a normal **TableDefinition**, so it can be used anywhere one is expected (CloudFormation, CDK, Terraform, SST,
 * jest setup or a raw **TableClient**). Call **client** to get a client for each part.
 */
export class SingleTableDefinition<
  PK extends string,
  SK extends string,
  Parts extends TablePartInfo<any, any, any, any>,
> extends TableDefinition<SingleTableType<PK, SK>, SingleTableKeys<PK, SK>> {
  constructor(
    keyNames: { partitionKey: PK; sortKey: SK },
    public readonly parts: Parts[],
  ) {
    super(keyNames as any, {});
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
  ): TablePartClients<Parts, SingleTableDefinition<PK, SK, Parts>> {
    const tableClient = new TableClient(this, config);
    const parts: TablePartInfo<any, any, any, any>[] = this.parts;
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
