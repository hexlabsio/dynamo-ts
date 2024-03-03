import {
  DynamoQuerier,
  KeyCompare,
  QuerierInput,
  QuerierReturn,
} from './dynamo-querier.js';
import { DynamoScanner, ScanOptions, ScanReturn } from './dynamo-scanner.js';
import {
  DynamoTableKeyConfig,
  TableDefinition,
} from './table-builder/table-definition.js';
import { DynamoConfig } from './types/dynamo-config.js';

export default class IndexClient<TableConfig extends TableDefinition> {
  constructor(
    public readonly tableConfig: TableConfig,
    private readonly parentKeys: DynamoTableKeyConfig<TableConfig>,
    private readonly clientConfig: DynamoConfig,
  ) {}

  query<PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QuerierInput<TableConfig['type'], PROJECTION> = {},
  ): Promise<QuerierReturn<TableConfig['type'], PROJECTION>> {
    return new DynamoQuerier<TableConfig>(
      this.tableConfig,
      this.clientConfig,
    ).query(keys, options);
  }

  queryAll<PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QuerierInput<TableConfig['type'], PROJECTION> = {},
  ): Promise<QuerierReturn<TableConfig['type'], PROJECTION>> {
    return new DynamoQuerier(
      this.tableConfig,
      this.clientConfig,
      this.parentKeys,
    ).queryAll(keys, options);
  }

  scan<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<ScanReturn<TableConfig['type'], PROJECTION>> {
    return new DynamoScanner<TableConfig>(this.clientConfig).scan(options);
  }

  scanAll<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<Omit<ScanReturn<TableConfig['type'], PROJECTION>, 'next'>> {
    return new DynamoScanner<TableConfig>(this.clientConfig).scanAll(options);
  }
}
