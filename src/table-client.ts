import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import {
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
import ReturnValue = DocumentClient.ReturnValue;

export class TableClient<T extends DynamoInfo> {
  constructor(public readonly info: T, private readonly config: DynamoConfig) {}

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

  get<PROJECTION = null>(
    keys: PickKeys<T>,
    options: GetItemOptions<T, PROJECTION> = {},
  ): Promise<GetItemReturn<T, PROJECTION>> {
    return new DynamoGetter(this.info, this.config).get(keys, options);
  }

  put<RETURN extends PutReturnValues = 'NONE'>(
    item: TypeFromDefinition<T['definition']>,
    options: PutItemOptions<T, RETURN> = {},
  ): Promise<PutItemReturn<T, RETURN>> {
    return new DynamoPuter(this.info, this.config).put(item, options);
  }

  delete<RETURN extends DeleteReturnValues = 'NONE'>(
    keys: PickKeys<T>,
    options: DeleteItemOptions<T, RETURN> = {},
  ): Promise<DeleteItemReturn<T, RETURN>> {
    return new DynamoDeleter(this.info, this.config).delete(keys, options);
  }

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

  update<
    KEY extends keyof DynamoNestedKV<TypeFromDefinition<T['definition']>>,
    RETURN_ITEMS extends ReturnValue | null = null,
  >(
    options: UpdateItemOptions<T, KEY, RETURN_ITEMS>,
  ): Promise<UpdateResult<T, RETURN_ITEMS>> {
    return new DynamoUpdater(this.info, this.config).update(options);
  }

  batchGet<PROJECTION = null>(
    keys: PickKeys<T>[],
    options: BatchGetItemOptions<T, PROJECTION> = {},
  ): BatchGetExecutor<T, PROJECTION> {
    return new DynamoBatchGetter(this.info, this.config).batchGetExecutor(
      keys,
      options,
    );
  }

  batchPut(
    items: TypeFromDefinition<T['definition']>[],
    options: BatchWriteItemOptions<T> = {},
  ): BatchWriteExecutor<T> {
    return new DynamoBatchWriter(this.config).batchPutExecutor(items, options);
  }

  batchDelete(
    keys: PickKeys<T>[],
    options: BatchWriteItemOptions<T> = {},
  ): BatchWriteExecutor<T> {
    return new DynamoBatchWriter(this.config).batchDeleteExecutor(
      keys,
      options,
    );
  }

  index<Index extends keyof T['indexes']>(
    indexName: Index,
  ): IndexClient<T['indexes'][Index] & { definition: T['definition'] }, T> {
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
