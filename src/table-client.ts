import { UpdateCommandInput } from "@aws-sdk/lib-dynamodb";
import {
  BatchWriteClient,
  BatchWriteExecutor,
  BatchWriteItemOptions,
  DynamoBatchWriter,
} from './dynamo-batch-writer';
import {
  DeleteItemOptions,
  DeleteItemReturn,
  DeleteReturnValues,
  DynamoDeleter,
} from './dynamo-deleter';
import { DynamoGetter, GetItemOptions, GetItemReturn } from './dynamo-getter';
import {
  DynamoBatchGetter,
  BatchGetExecutor,
  BatchGetItemOptions,
} from './dynamo-batch-getter';
import {
  DynamoPuter,
  PutItemOptions,
  PutItemReturn,
  PutReturnValues,
} from './dynamo-puter';
import {
  DynamoQuerier,
  QuerierInput,
  QuerierReturn,
  QueryKeys,
} from './dynamo-querier';
import { DynamoScanner, ScanOptions, ScanReturn } from './dynamo-scanner';
import {
  DynamoUpdater,
  UpdateItemOptions,
  UpdateResult,
} from './dynamo-updater';
import IndexClient from './index-client';
import { DynamoNestedKV } from './type-mapping';
import {
  DynamoConfig,
  DynamoInfo,
  PickKeys,
  TypeFromDefinition,
} from './types';

export class TableClient<T extends DynamoInfo> {
  constructor(public readonly info: T, private readonly config: DynamoConfig) {}

  /**
   * Scans an entire table, use filter to narrow the results however the filter will be applied after the results have been returned.
   * @returns - A list of items (1 page only)
   */
  scan<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION> = {},
  ): Promise<ScanReturn<T, PROJECTION>> {
    return new DynamoScanner(this.info, this.config).scan(options);
  }

  /**
   * Scans an entire table, use filter to narrow the results however the filter will be applied after the results have been returned.
   * @returns - A list of all items
   */
  scanAll<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION> = {},
  ): Promise<Omit<ScanReturn<T, PROJECTION>, 'next'>> {
    return new DynamoScanner(this.info, this.config).scanAll(options);
  }

  /**
   * Returns an item that matches the given keys or **undefined** if not present
   * @returns - The item or **undefined**
   */
  get<PROJECTION = null>(
    keys: PickKeys<T>,
    options: GetItemOptions<T, PROJECTION> = {},
  ): Promise<GetItemReturn<T, PROJECTION>> {
    return new DynamoGetter(this.info, this.config).get(keys, options);
  }

  /**
   * Creates or replaces an item that matches the same keys
   * @returns - When returnValues is set to ALL_OLD, the previous item will be returned if present.
   */
  put<RETURN extends PutReturnValues = 'NONE'>(
    item: TypeFromDefinition<T['definition']>,
    options: PutItemOptions<T, RETURN> = {},
  ): Promise<PutItemReturn<T, RETURN>> {
    return new DynamoPuter(this.info, this.config).put(item, options);
  }

  /**
   * Deletes an item that matches the same keys, use condition if you want to cause it to fail under certain conditions
   * @returns - When returnValues is set to ALL_OLD, the previous item will be returned if present.
   */
  delete<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: PickKeys<T>,
    options: DeleteItemOptions<T, RETURN> = {},
  ): Promise<DeleteItemReturn<T, RETURN>> {
    return new DynamoDeleter(this.info, this.config).delete(keys, options);
  }

  /**
   * Queries a partition with any given key conditions and filters
   * @returns - A list of results (1 page only).
   */
  query<PROJECTION = null>(
    keys: QueryKeys<T>,
    options: QuerierInput<T, PROJECTION> = {},
  ): Promise<QuerierReturn<T, PROJECTION>> {
    return new DynamoQuerier(this.info, this.config).query(keys, options);
  }

  /**
   * Queries a partition with any given key conditions and filters. All pages will be returned.
   * @returns - A list of results.
   */
  queryAll<PROJECTION = null>(
    keys: QueryKeys<T>,
    options: QuerierInput<T, PROJECTION> = {},
  ): Promise<Omit<QuerierReturn<T, PROJECTION>, 'next'>> {
    return new DynamoQuerier(this.info, this.config).queryAll(keys, options);
  }

  /**
   * Updates an item that matches the same key or creates a new item if it did not exist.
   *
   * Any keys that are sent will be updated, others will be left unaffected. Any values that are undefined will result in a REMOVE.
   *
   * Use **increments** to set which fields should be atomically incremented (or decremented).
   *
   * @returns - When returnValues is set to ALL_OLD, the previous item will be returned if present.
   */
  update<
    KEY extends keyof DynamoNestedKV<TypeFromDefinition<T['definition']>>,
    RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null = null,
  >(
    options: UpdateItemOptions<T, KEY, RETURN_ITEMS>,
  ): Promise<UpdateResult<T, RETURN_ITEMS>> {
    return new DynamoUpdater(this.info, this.config).update(options);
  }

  /**
   * Allows up to 25 get requests to be sent in one request.
   *
   * @returns - An executor that can be executed, or you can append more requests from other tables by calling **and()**.
   */
  batchGet<PROJECTION = null>(
    keys: PickKeys<T>[],
    options: BatchGetItemOptions<T, PROJECTION> = {},
  ): BatchGetExecutor<T, PROJECTION> {
    return new DynamoBatchGetter(this.info, this.config).batchGetExecutor(
      keys,
      options,
    );
  }

  /**
   * Allows up to 25 write requests to be sent in one request. This will start with PUT requests, but you can add DELETE requests too by calling **and()**
   *
   * @returns - An executor that can be executed, or you can append more PUT or DELETE requests from other tables by calling **and()**.
   */
  batchPut(
    items: TypeFromDefinition<T['definition']>[],
    options: BatchWriteItemOptions<T> = {},
  ): BatchWriteClient<[BatchWriteExecutor<T>]> {
    return new DynamoBatchWriter(this.config).batchPutExecutor(items, options);
  }

  /**
   * Allows up to 25 write requests to be sent in one request. This will start with DELETE requests, but you can add PUT requests too by calling **and()**
   *
   * @returns - An executor that can be executed, or you can append more PUT or DELETE requests from other tables by calling **and()**.
   */
  batchDelete(
    keys: PickKeys<T>[],
    options: BatchWriteItemOptions<T> = {},
  ): BatchWriteClient<[BatchWriteExecutor<T>]> {
    return new DynamoBatchWriter(this.config).batchDeleteExecutor(
      keys,
      options,
    );
  }

  /**
   * Selects an index to query
   */
  index<Index extends keyof T['indexes']>(
    indexName: Index,
  ): IndexClient<
    T['definition'],
    T['indexes'][Index] & { definition: T['definition'] },
    T
  > {
    return new IndexClient(
      this.info,
      { ...this.info, ...this.info.indexes[indexName] },
      { ...this.config, indexName: indexName as string },
    );
  }

  static build<T extends DynamoInfo>(
    params: T,
    config: DynamoConfig,
  ): TableClient<T> {
    return new TableClient<T>(params, config);
  }
}
