import { BatchGetItemOptions } from './dynamo-batch-getter';
import { GetItemOptions } from './dynamo-getter';
import { ScanOptions } from './dynamo-scanner';
import { UpdateItemOptions } from './dynamo-updater';
import { TableClient } from './table-client';
import { DynamoNestedKV } from './type-mapping';
import { DynamoInfo, DynamoTypeFrom, PickKeys, TypeFromDefinition } from './types';
import { v4 as uuid } from 'uuid';

export type ProjectionOrTypeArray<PROJECTION, T extends DynamoInfo> = PROJECTION extends null
  ? TypeFromDefinition<T['definition']>[]
  : PROJECTION[]

export type ProjectionOrType<PROJECTION, T extends DynamoInfo> = PROJECTION extends null
  ? TypeFromDefinition<T['definition']>
  : PROJECTION
export class Crud<TABLE extends DynamoInfo> {
  constructor(
    protected readonly tableClient: TableClient<TABLE>
  ) {}

  async readAll<PROJECTION = null>(
    options: ScanOptions<TABLE, PROJECTION> = {},
  ): Promise<ProjectionOrTypeArray<PROJECTION, TABLE>> {
    const result = await this.tableClient.scanAll(options);
    return result.member;
  }

  async readMany<PROJECTION = null>(
    keys: PickKeys<TABLE>[],
    options: BatchGetItemOptions<TABLE, PROJECTION> = {},
  ): Promise<ProjectionOrTypeArray<PROJECTION, TABLE>> {
    const result = await this.tableClient.batchGet(keys, options).execute();
    return result.items;
  }

  async read<PROJECTION = null>(
    keys: PickKeys<TABLE>,
    options: GetItemOptions<TABLE, PROJECTION> = {},
  ): Promise<ProjectionOrType<PROJECTION, TABLE> | undefined> {
    const result = await this.tableClient.get(keys, options)
    return result.item;
  }

  async create(
    item: Omit<TypeFromDefinition<TABLE['definition']>, TABLE['partitionKey']>
  ): Promise<DynamoTypeFrom<TABLE>> {
    const identifier = uuid();
    const actualItem = { [this.tableClient.info.partitionKey]: identifier, ...item, };
    await this.tableClient.put(actualItem as any);
    return actualItem as any;
  }

  async update<
    KEY extends keyof DynamoNestedKV<TypeFromDefinition<TABLE['definition']>>
  >(
    options: Omit<UpdateItemOptions<TABLE, KEY, 'ALL_NEW'>, 'return'>,
  ): Promise<DynamoTypeFrom<TABLE>> {
    const result = await this.tableClient.update({ ...options, return: 'ALL_NEW' });
    return result.item as any;
  }

  async deleteItem(keys: PickKeys<TABLE>) {
    await this.tableClient.delete(keys);
  }

}
