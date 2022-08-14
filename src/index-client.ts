import {
  DynamoQuerier,
  QuerierInput,
  QuerierReturn,
  QueryKeys,
} from './dynamo-querier';
import { DynamoScanner, ScanOptions, ScanReturn } from './dynamo-scanner';
import { DynamoConfig, DynamoInfo } from './types';

export default class IndexClient<
  T extends DynamoInfo,
  Parent extends DynamoInfo,
> {
  constructor(
    public readonly parent: Parent,
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
  ): Promise<Omit<QuerierReturn<T, PROJECTION>, 'next'>> {
    return new DynamoQuerier(this.info, this.config).query(keys, options);
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
