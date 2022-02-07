import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import { DynamoClientConfig } from './dynamo-client-config';
import {
  DynamoEntry,
  DynamoMapDefinition,
  DynamoRangeKey,
} from './type-mapping';

import WriteRequests = DocumentClient.WriteRequests;

type PutWrite<DEFINITION extends DynamoMapDefinition> = {
  put: DynamoEntry<DEFINITION>;
};
type DeleteWrite<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends DynamoRangeKey<DEFINITION, HASH> | null = null,
> = {
  delete: DeleteWriteItem<DEFINITION, HASH, RANGE>;
};
export type DeleteWriteItem<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends DynamoRangeKey<DEFINITION, HASH> | null = null,
> = { [K in RANGE extends string ? HASH | RANGE : HASH]: DEFINITION[K] };
export type BatchWrite<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends DynamoRangeKey<DEFINITION, HASH> | null = null,
> = DeleteWrite<DEFINITION, HASH, RANGE> | PutWrite<DEFINITION>;

export type BatchWriteOutput = { unprocessed?: WriteRequests };
export class DynamoBatchWriter {
  private static async directBatchWrite<DEFINITION extends DynamoMapDefinition>(
    config: DynamoClientConfig<DEFINITION>,
    writeRequest: WriteRequests,
  ): Promise<BatchWriteOutput> {
    const batchWriteInput = {
      RequestItems: { [config.tableName]: writeRequest },
    };
    if (config.logStatements) {
      console.log(JSON.stringify(batchWriteInput, null, 2));
    }
    const result = await config.client.batchWrite(batchWriteInput).promise();
    return { unprocessed: result.UnprocessedItems?.[config.tableName] };
  }

  private static chunkArray<U>(
    u: U[],
    chunkSize: number,
    acc: U[][] = [],
  ): U[][] {
    acc.push(u.slice(0, chunkSize));
    const rest = u.slice(chunkSize, u.length);
    return rest.length > 0 ? this.chunkArray(rest, chunkSize, acc) : acc;
  }

  private static isPutWrite<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends DynamoRangeKey<DEFINITION, HASH> | null = null,
  >(
    batchWrite: BatchWrite<DEFINITION, HASH, RANGE>,
  ): batchWrite is PutWrite<DEFINITION> {
    return (batchWrite as PutWrite<DEFINITION>).put !== undefined;
  }

  static async batchWrite<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends DynamoRangeKey<DEFINITION, HASH> | null = null,
  >(
    config: DynamoClientConfig<DEFINITION>,
    operations: BatchWrite<DEFINITION, HASH, RANGE>[],
  ): Promise<BatchWriteOutput> {
    const directWriteOps = operations.map((operation) =>
      this.isPutWrite(operation)
        ? { PutRequest: { Item: operation.put } }
        : { Delete: { Key: operation.delete } },
    );

    const opCount = directWriteOps.length;
    if (opCount === 0) {
      return {};
    } else if (directWriteOps.length <= 25) {
      return await this.directBatchWrite(config, directWriteOps);
    } else {
      const chunkedDirectWriteOps = this.chunkArray(directWriteOps, 25);

      const res = await Promise.all(
        chunkedDirectWriteOps.map((it) => this.directBatchWrite(config, it)),
      );
      return { unprocessed: res.flatMap((it) => it.unprocessed ?? []) };
    }
  }

  static async batchPut<DEFINITION extends DynamoMapDefinition>(
    config: DynamoClientConfig<DEFINITION>,
    operations: DynamoEntry<DEFINITION>[],
  ): Promise<BatchWriteOutput> {
    return await this.batchWrite(
      config,
      operations.map((it) => ({ put: it })),
    );
  }

  static async batchDelete<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends DynamoRangeKey<DEFINITION, HASH> | null = null,
  >(
    config: DynamoClientConfig<DEFINITION>,
    operations: DeleteWriteItem<DEFINITION, HASH, RANGE>[],
  ): Promise<BatchWriteOutput> {
    return await this.batchWrite(
      config,
      operations.map((it) => ({ delete: it })),
    );
  }
}
