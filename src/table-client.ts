import { BatchWriteExecutor, BatchWriteItemOptions, DynamoBatchWriter } from './dynamo-batch-writer';
import { DeleteItemOptions, DeleteItemReturn, DeleteReturnValues, DynamoDeleter } from './dynamo-deleter';
import { DynamoGetter, GetItemOptions, GetItemReturn } from './dynamo-getter';
import { DynamoBatchGetter, BatchGetExecutor, BatchGetItemOptions } from './dynamo-batch-getter';
import { DynamoPuter, PutItemOptions, PutItemReturn, PutReturnValues } from './dynamo-puter';
import { DynamoQuerier, QuerierInput, QuerierReturn, QueryKeys } from './dynamo-querier';
import { DynamoConfig, DynamoInfo, PickKeys, TypeFromDefinition } from './types';

export default class TableClient<T extends DynamoInfo> {

  constructor(public readonly info: T, private readonly config: DynamoConfig) {}

  get<PROJECTION = null>(keys: PickKeys<T>, options: GetItemOptions<T, PROJECTION> = {}): Promise<GetItemReturn<T, PROJECTION>> {
    return new DynamoGetter(this.info, this.config).get(keys, options);
  }

  put<RETURN extends PutReturnValues = "NONE">(item: TypeFromDefinition<T['definition']>, options: PutItemOptions<T, RETURN> = {}): Promise<PutItemReturn<T, RETURN>> {
    return new DynamoPuter(this.info, this.config).put(item, options);
  }

  delete<RETURN extends DeleteReturnValues = "NONE">(keys: PickKeys<T>, options: DeleteItemOptions<T, RETURN> = {}): Promise<DeleteItemReturn<T, RETURN>> {
    return new DynamoDeleter(this.info, this.config).delete(keys, options);
  }

  query<PROJECTION = null>(keys: QueryKeys<T>, options: QuerierInput<T, PROJECTION> = {}): Promise<QuerierReturn<T, PROJECTION>> {
    return new DynamoQuerier(this.info, this.config).query(keys, options);
  }

  batchGet<PROJECTION = null>(keys: PickKeys<T>[], options: BatchGetItemOptions<T, PROJECTION> = {}): BatchGetExecutor<T, PROJECTION> {
    return new DynamoBatchGetter(this.info, this.config).batchGetExecutor(keys, options);
  }

  batchPut(items: TypeFromDefinition<T['definition']>[], options: BatchWriteItemOptions<T> = {}): BatchWriteExecutor<T> {
    return new DynamoBatchWriter(this.config).batchPutExecutor(items, options);
  }

  batchDelete(keys: PickKeys<T>[], options: BatchWriteItemOptions<T> = {}): BatchWriteExecutor<T> {
    return new DynamoBatchWriter(this.config).batchDeleteExecutor(keys, options);
  }
}
