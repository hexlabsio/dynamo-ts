import {DynamoEntry, DynamoKeysFrom, DynamoMapDefinition} from "./type-mapping";
import {DynamoClientConfig, DynamoDefinition} from "./dynamo-client-config";
import {DynamoGetter, GetItemExtras} from "./dynamo-getter";
import {DynamoPutter, PutItemExtras} from "./dynamo-putter";
import {AttributeBuilder} from "./naming";
import {DynamoQuerier, QueryParametersInput} from "./dynamo-querier";

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
    INDEXES extends Record<string, { local?: boolean, hashKey: keyof DynamoEntry<DEFINITION>; rangeKey: keyof DynamoEntry<DEFINITION> }> | null = null,
> implements Queryable<DEFINITION, HASH, RANGE>
{
  constructor(
    protected readonly config: DynamoClientConfig<DEFINITION>,
    protected readonly hash: HASH,
    protected readonly range: RANGE,
    protected readonly indexes: INDEXES | null
  ) {}

  async get(key: DynamoKeysFrom<DEFINITION, HASH, RANGE>, options: GetItemExtras = {}): Promise<{[K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K]} | undefined> {
    return DynamoGetter.get(this.config, key, options);
  }

  async put<RETURN_OLD extends boolean = false>(item: DynamoEntry<DEFINITION>, options: PutItemExtras<DEFINITION, HASH, RANGE, RETURN_OLD> = {}) : Promise<RETURN_OLD extends true ? { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] } : void> {
    return DynamoPutter.put(this.config, {definition: this.config.definition, hash: this.hash, range: this.range}, AttributeBuilder.create(), item, options);
  }

  async query(
      options: QueryParametersInput<DEFINITION, HASH, RANGE>
  ): Promise<{
    next?: string;
    member: { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }[];
  }> {
    return DynamoQuerier.query(this.config, {definition: this.config.definition, hash: this.hash, range: this.range}, AttributeBuilder.create(), options);
  }

  index<INDEX extends keyof INDEXES>(index: INDEX): INDEXES extends {} ? Queryable<DEFINITION, INDEXES[INDEX]['hashKey'], INDEXES[INDEX]['rangeKey']> : never {
    if(this.indexes) {
      const {hashKey, rangeKey} = this.indexes[index as string];
      return TableClient.build({definition: this.config.definition, hash: hashKey, range: rangeKey as any}, {
        ...this.config,
        indexName: index as string
      }, null) as any;
    }
    return undefined as any;
  }

  static build<
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null,
      INDEXES extends Record<string, { local?: boolean, hashKey: keyof DynamoEntry<DEFINITION>; rangeKey: keyof DynamoEntry<DEFINITION> }> | null = null,
      >
    (
          definition: DynamoDefinition<DEFINITION, HASH, RANGE>,
          config: Omit<DynamoClientConfig<DEFINITION>, 'tableType' | 'definition'>,
          indexes?: INDEXES
    ): TableClient<DEFINITION, HASH, RANGE, INDEXES>{
    return new TableClient({...config, tableType: undefined as any, definition: definition.definition}, definition.hash, definition.range, indexes as any);
  }
}

export function defineTable<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null
    >(
    definition: DEFINITION,
    hash: HASH,
    range: RANGE = null as RANGE
): DynamoDefinition<DEFINITION, HASH, RANGE> {
  return {definition, hash, range: range as RANGE};
}