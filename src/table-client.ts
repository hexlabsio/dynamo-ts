import {DynamoEntry, DynamoKeysFrom, DynamoMapDefinition} from "./type-mapping";
import {DynamoClientConfig, DynamoDefinition} from "./dynamo-client-config";
import {DynamoGetter, GetItemExtras} from "./dynamo-getter";
import {DynamoPutter, PutItemExtras} from "./dynamo-putter";
import {AttributeBuilder} from "./naming";

export class TableClient<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
>
{
  constructor(
    protected readonly config: DynamoClientConfig<DEFINITION>,
    protected readonly hash: HASH,
    protected readonly range: RANGE
  ) {}

  async get(key: DynamoKeysFrom<DEFINITION, HASH, RANGE>, options: GetItemExtras = {}): Promise<{[K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K]} | undefined> {
    return DynamoGetter.get(this.config, key, options);
  }

  async put<RETURN_OLD extends boolean = false>(item: DynamoEntry<DEFINITION>, options: PutItemExtras<DEFINITION, HASH, RANGE, RETURN_OLD> = {}) : Promise<RETURN_OLD extends true ? { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] } : void> {
    return DynamoPutter.put(this.config, {definition: this.config.definition, hash: this.hash, range: this.range}, AttributeBuilder.create(), item, options);
  }

  static build<
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null,
      >(definition: DynamoDefinition<DEFINITION, HASH, RANGE>, config: Omit<DynamoClientConfig<DEFINITION>, 'tableType' | 'definition'>):
      TableClient<DEFINITION, HASH, RANGE>{
    return new TableClient({...config, tableType: undefined as any, definition: definition.definition}, definition.hash, definition.range);
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