import {
  DynamoEntry,
  DynamoIndexes,
  DynamoKeysFrom,
  DynamoMapDefinition,
} from "./type-mapping";
import {DynamoClientConfig, DynamoDefinition} from "./dynamo-client-config";
import {DynamoGetter, GetItemExtras} from "./dynamo-getter";
import {DynamoPutter, PutItemExtras} from "./dynamo-putter";
import {DynamoQuerier, QueryParametersInput} from "./dynamo-querier";
import {DocumentClient} from "aws-sdk/lib/dynamodb/document_client";
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import {DynamoScanner, ScanOptions} from "./dynamo-scanner";
import {DeleteItemOptions, DynamoDeleter} from "./dynamo-deleter";
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;
import {DynamoUpdater, UpdateItemOptions, UpdateReturnType} from "./dynamo-updater";
import ReturnValue = DocumentClient.ReturnValue;

export interface Queryable<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    > {
  query(
      options: QueryParametersInput<DEFINITION, HASH, RANGE>
  ): Promise<{
    next?: string;
    member: { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }[];
  }>
}

export class TableClient<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    INDEXES extends DynamoIndexes<DEFINITION> = null,
> implements Queryable<DEFINITION, HASH, RANGE>
{
  constructor(
    protected readonly config: DynamoClientConfig<DEFINITION>,
    protected readonly definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>
  ) {}

  async scan<R = null>(options: ScanOptions<DEFINITION, R> = {}): Promise<{
    next?: string;
    member: { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }[];
  }> {
    return DynamoScanner.scan(this.config, this.definition, options);
  }

  async get<R = null>(key: DynamoKeysFrom<DEFINITION, HASH, RANGE>, options: GetItemExtras<DEFINITION, R> = {}): Promise<{item: (R extends null ? DynamoClientConfig<DEFINITION>['tableType'] : R) | undefined, consumedCapacity?: ConsumedCapacity}> {
    return DynamoGetter.get(this.config, key, options);
  }

  async put<RETURN_OLD extends boolean = false>(item: DynamoEntry<DEFINITION>, options: PutItemExtras<DEFINITION, HASH, RANGE, RETURN_OLD> = {}) : Promise<RETURN_OLD extends true ? { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] } : void> {
    return DynamoPutter.put(this.config, this.definition, item, options);
  }

  async query<R = null>(
      options: QueryParametersInput<DEFINITION, HASH, RANGE, R>
  ): Promise<{
    next?: string;
    member: { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }[];
  }> {
    return DynamoQuerier.query(this.config, this.definition, options);
  }

  async update<KEY extends keyof DynamoEntry<DEFINITION>, RETURN_ITEMS extends ReturnValue | null = null> (
      options: UpdateItemOptions<DEFINITION, HASH, RANGE, KEY, RETURN_ITEMS>
  ) : Promise<{item: UpdateReturnType<DEFINITION, RETURN_ITEMS>, consumedCapacity?: ConsumedCapacity, itemCollectionMetrics?: ItemCollectionMetrics}> {
    return DynamoUpdater.update(this.config, options);
  }


    async delete<RETURN_OLD extends boolean = false> (
      key: DynamoKeysFrom<DEFINITION, HASH, RANGE>,
      options: DeleteItemOptions<DEFINITION, RETURN_OLD>
  ) : Promise<{consumedCapacity?: ConsumedCapacity, itemCollectionMetrics?: ItemCollectionMetrics} & (RETURN_OLD extends true ? {item: DynamoClientConfig<DEFINITION>['tableType']} : {})> {
    return DynamoDeleter.delete(this.config, key, options);

  }
  index<INDEX extends keyof INDEXES>(index: INDEX): INDEXES extends {} ? Queryable<DEFINITION, INDEXES[INDEX]['hashKey'], INDEXES[INDEX]['rangeKey']> : never {
    if(this.definition.indexes) {
      const {hashKey, rangeKey} = this.definition.indexes[index as string];
      return TableClient.build({definition: this.config.definition, hash: hashKey, range: rangeKey as any, indexes: null}, {
        ...this.config,
        indexName: index as string
      }) as any;
    }
    return undefined as any;
  }

  static build<
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null,
      INDEXES extends DynamoIndexes<DEFINITION> = null,
      >
    (
          definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
          config: Omit<DynamoClientConfig<DEFINITION>, 'tableType' | 'definition'>
    ): TableClient<DEFINITION, HASH, RANGE, INDEXES>{

    return new TableClient({...config, tableType: undefined as any, definition: definition.definition}, definition);
  }
}

export function defineTable<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    INDEXES extends Record<string, { local?: boolean, hashKey: keyof DynamoEntry<DEFINITION>; rangeKey: keyof DynamoEntry<DEFINITION> | null }> | null = null
    >(
    definition: DEFINITION,
    hash: HASH,
    range: RANGE = null as RANGE,
    indexes: INDEXES = null as INDEXES
): DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES> {
  return {definition, hash, range: range as RANGE, indexes: indexes};
}