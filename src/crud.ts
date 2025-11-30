import { BatchGetItemOptions } from './dynamo-batch-getter.js';
import { GetItemOptions } from './dynamo-getter.js';
import { ScanOptions } from './dynamo-scanner.js';
import { UpdateItemOptions } from './dynamo-updater.js';
import { TableDefinition } from './table-builder/table-definition.js';
import { TableClient } from './table-client.js';
import { randomUUID } from 'node:crypto';
import { JsonPath } from './types/index.js';

export type ProjectionOrTypeArray<PROJECTION, TableType> =
  PROJECTION extends null ? TableType[] : PROJECTION[];

export type ProjectionOrType<PROJECTION, TableType> = PROJECTION extends null
  ? TableType
  : PROJECTION;
export class Crud<TableConfig extends TableDefinition> {
  constructor(protected readonly tableClient: TableClient<TableConfig>) {}

  async readAll<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<ProjectionOrTypeArray<PROJECTION, TableConfig['type']>> {
    const result = await this.tableClient.scanAll(options);
    return result.member;
  }

  async readMany<PROJECTION = null>(
    keys: TableConfig['keys'][],
    options: BatchGetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<ProjectionOrTypeArray<PROJECTION, TableConfig['type']>> {
    const result = await this.tableClient.batchGet(keys, options).execute();
    return result.items;
  }

  async read<PROJECTION = null>(
    keys: TableConfig['keys'],
    options: GetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<ProjectionOrType<PROJECTION, TableConfig['type']> | undefined> {
    const result = await this.tableClient.get(keys, options);
    return result.item;
  }

  async create(
    item: Omit<TableConfig['type'], TableConfig['keyNames']['partitionKey']>,
  ): Promise<TableConfig['type']> {
    const identifier = randomUUID();
    const actualItem = {
      [this.tableClient.tableConfig.keyNames.partitionKey]: identifier,
      ...item,
    };
    await this.tableClient.put(actualItem as any);
    return actualItem as any;
  }

  async update<KEY extends JsonPath<TableConfig['type']>>(
    options: Omit<
      UpdateItemOptions<TableConfig['type'], KEY, 'ALL_NEW'>,
      'return'
    >,
  ): Promise<TableConfig['type']> {
    const result = await this.tableClient.update({
      ...options,
      return: 'ALL_NEW',
    });
    return result.item as any;
  }

  async deleteItem(keys: TableConfig['keys']) {
    await this.tableClient.delete(keys);
  }
}
