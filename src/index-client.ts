import {
  DynamoQuerier,
  QuerierInput,
  QuerierReturn,
  QueryKeys,
} from './dynamo-querier';
import { DynamoScanner, ScanOptions, ScanReturn } from './dynamo-scanner';
import { DynamoConfig, DynamoDefinition, DynamoInfo } from './types';

export default class IndexClient<
  D extends DynamoDefinition,
  T extends DynamoInfo<D>,
  P extends DynamoInfo<D>,
> {
  constructor(
    public readonly parent: P,
    public readonly info: T,
    private readonly config: DynamoConfig,
  ) {}

  query<PROJECTION = null>(
    keys: QueryKeys<T>,
    options: QuerierInput<T, PROJECTION> = {},
  ): Promise<QuerierReturn<T, PROJECTION>> {
    return new DynamoQuerier(this.info, this.config).query(keys, options);
  }

  queryAll<PROJECTION = null>(
    keys: QueryKeys<T>,
    options: QuerierInput<T, PROJECTION> = {},
  ): Promise<QuerierReturn<T, PROJECTION>> {
    const pk: keyof D = this.parent.partitionKey;
    const sk: keyof D | undefined =
      this.parent?.sortKey === null ? undefined : this.parent?.sortKey;
    return new DynamoQuerier(this.info, this.config, {
      partitionKey: pk,
      sortKey: sk,
    }).queryAll(keys, options);
  }

  scan<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION> = {},
  ): Promise<ScanReturn<T, PROJECTION>> {
    return new DynamoScanner(this.info, this.config).scan(options);
  }

  scanAll<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION> = {},
  ): Promise<Omit<ScanReturn<T, PROJECTION>, 'next'>> {
    return new DynamoScanner(this.info, this.config).scanAll(options);
  }
}
