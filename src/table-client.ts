import { DynamoGetter, GetItemOptions, GetItemReturn } from './dynamo-getter';
import { DynamoBatchGetter, BatchGetExecutor, BatchGetItemOptions } from './dynamo-batch-getter';
import { DynamoConfig, DynamoInfo, PickKeys } from './types';

export default class TableClient<T extends DynamoInfo> {

  constructor(public readonly info: T, private readonly config: DynamoConfig) {}

  get<PROJECTION = null>(keys: PickKeys<T>, options: GetItemOptions<T, PROJECTION> = {}): Promise<GetItemReturn<T, PROJECTION>> {
    return new DynamoGetter(this.info, this.config).get(keys, options);
  }

  batchGet<PROJECTION = null>(keys: PickKeys<T>[], options: BatchGetItemOptions<T, PROJECTION> = {}): BatchGetExecutor<T, PROJECTION> {
    return new DynamoBatchGetter(this.info, this.config).batchGetExecutor(keys, options);
  }
}
