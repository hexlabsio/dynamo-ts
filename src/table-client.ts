import { UpdateCommandInput } from '@aws-sdk/lib-dynamodb';
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
  KeyCompare,
  QuerierInput,
  QuerierReturn,
} from './dynamo-querier';
import { DynamoScanner, ScanOptions, ScanReturn } from './dynamo-scanner';
import { DynamoTransactGetter } from './dynamo-transact-getter';
import { DynamoTransactWriter } from './dynamo-transact-writer';
import {
  DynamoUpdater,
  UpdateItemOptions,
  UpdateResult,
} from './dynamo-updater';
import IndexClient from './index-client';
import { TableDefinition } from './table-builder/table-definition';
import { DynamoConfig, JsonPath } from './types';

export class TableClient<TableConfig extends TableDefinition> {
  constructor(
    public readonly tableConfig: TableConfig,
    private readonly clientConfig: DynamoConfig,
  ) {
    const writer = new DynamoTransactWriter(clientConfig);
    const getter = new DynamoTransactGetter(clientConfig);

    this.transaction = new Proxy(this, {
      get(
        target: TableClient<TableConfig>,
        p: string | symbol,
        receiver: any,
      ): any {
        if (p === 'get') return getter.get.bind(getter);
        else return (writer as any)[p].bind(writer);
      },
    }) as any;
  }

  transaction: {
    get: DynamoTransactGetter<TableConfig>['get'];
    put: DynamoTransactWriter<TableConfig>['put'];
    update: DynamoTransactWriter<TableConfig>['update'];
    delete: DynamoTransactWriter<TableConfig>['delete'];
    conditionCheck: DynamoTransactWriter<TableConfig>['conditionCheck'];
  };

  /**
   * Scans an entire table, use filter to narrow the results however the filter will be applied after the results have been returned.
   * @returns - A list of items (1 page only)
   */
  scan<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<ScanReturn<TableConfig['type'], PROJECTION>> {
    return new DynamoScanner<TableConfig>(this.clientConfig).scan(options);
  }

  /**
   * Scans an entire table, use filter to narrow the results however the filter will be applied after the results have been returned.
   * @returns - A list of all items
   */
  scanAll<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<Omit<ScanReturn<TableConfig['type'], PROJECTION>, 'next'>> {
    return new DynamoScanner<TableConfig>(this.clientConfig).scanAll(options);
  }

  /**
   * Returns an item that matches the given keys or **undefined** if not present
   * @returns - The item or **undefined**
   */
  get<PROJECTION = null>(
    keys: TableConfig['keys'],
    options: GetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<GetItemReturn<TableConfig['type'], PROJECTION>> {
    return new DynamoGetter<TableConfig>(this.clientConfig).get(keys, options);
  }

  /**
   * Creates or replaces an item that matches the same keys
   * @returns - When returnValues is set to ALL_OLD, the previous item will be returned if present.
   */
  put<RETURN extends PutReturnValues = 'NONE'>(
    item: TableConfig['type'],
    options: PutItemOptions<TableConfig['type'], RETURN> = {},
  ): Promise<PutItemReturn<TableConfig['type'], RETURN>> {
    return new DynamoPuter<TableConfig>(this.clientConfig).put(item, options);
  }

  /**
   * Deletes an item that matches the same keys, use condition if you want to cause it to fail under certain conditions
   * @returns - When returnValues is set to ALL_OLD, the previous item will be returned if present.
   */
  delete<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: TableConfig['keys'],
    options: DeleteItemOptions<TableConfig['type'], RETURN> = {},
  ): Promise<DeleteItemReturn<TableConfig['type'], RETURN>> {
    return new DynamoDeleter<TableConfig>(this.clientConfig).delete(
      keys,
      options,
    );
  }

  /**
   * Queries a partition with any given key conditions and filters
   * @returns - A list of results (1 page only).
   */
  query<PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QuerierInput<TableConfig['type'], PROJECTION> = {},
  ): Promise<QuerierReturn<TableConfig['type'], PROJECTION>> {
    return new DynamoQuerier(this.tableConfig, this.clientConfig).query(
      keys,
      options,
    );
  }

  /**
   * Queries a partition with any given key conditions and filters. All pages will be returned.
   * @returns - A list of results.
   */
  queryAll<PROJECTION = null>(
    keys: KeyCompare<TableConfig['type'], TableConfig['keyNames']>,
    options: QuerierInput<TableConfig['type'], PROJECTION> = {},
  ): Promise<Omit<QuerierReturn<TableConfig['type'], PROJECTION>, 'next'>> {
    return new DynamoQuerier(this.tableConfig, this.clientConfig).queryAll(
      keys,
      options,
    );
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
    KEY extends JsonPath<TableConfig['type']>,
    RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null = null,
  >(
    options: UpdateItemOptions<TableConfig['type'], KEY, RETURN_ITEMS>,
  ): Promise<UpdateResult<TableConfig['type'], RETURN_ITEMS>> {
    return new DynamoUpdater<TableConfig>(this.clientConfig).update(options);
  }

  /**
   * Allows up to 25 get requests to be sent in one request.
   *
   * @returns - An executor that can be executed, or you can append more requests from other tables by calling **and()**.
   */
  batchGet<PROJECTION = null>(
    keys: TableConfig['keys'][],
    options: BatchGetItemOptions<TableConfig['type'], PROJECTION> = {},
  ): BatchGetExecutor<TableConfig['type'], PROJECTION> {
    return new DynamoBatchGetter<TableConfig>(
      this.clientConfig,
    ).batchGetExecutor(keys, options);
  }

  /**
   * Allows up to 25 write requests to be sent in one request. This will start with PUT requests, but you can add DELETE requests too by calling **and()**
   *
   * @returns - An executor that can be executed, or you can append more PUT or DELETE requests from other tables by calling **and()**.
   */
  batchPut(
    items: TableConfig['type'][],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    return new DynamoBatchWriter<TableConfig>(
      this.clientConfig,
    ).batchPutExecutor(items, options);
  }

  /**
   * Allows up to 25 write requests to be sent in one request. This will start with DELETE requests, but you can add PUT requests too by calling **and()**
   *
   * @returns - An executor that can be executed, or you can append more PUT or DELETE requests from other tables by calling **and()**.
   */
  batchDelete(
    keys: TableConfig['keys'][],
    options: BatchWriteItemOptions = {},
  ): BatchWriteClient<[BatchWriteExecutor]> {
    return new DynamoBatchWriter<TableConfig>(
      this.clientConfig,
    ).batchDeleteExecutor(keys, options);
  }

  /**
   * Selects an index to query
   */
  index<Index extends keyof TableConfig['indexes']>(
    indexName: Index,
  ): IndexClient<
    TableDefinition<TableConfig['type'], TableConfig['indexes'][Index]>
  > {
    return new IndexClient(
      (this.tableConfig as any).asIndex(indexName),
      this.tableConfig.keyNames,
      { ...this.clientConfig, indexName: indexName as string },
    );
  }

  static build<TableConfig extends TableDefinition>(
    tableConfig: TableConfig,
    clientConfig: DynamoConfig,
  ): TableClient<TableConfig> {
    return new TableClient<TableConfig>(tableConfig, clientConfig);
  }
}
