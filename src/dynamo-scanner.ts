import { DynamoDB } from 'aws-sdk';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import ConsumedCapacity = DynamoDB.DocumentClient.ConsumedCapacity;
import { AttributeBuilder } from './attribute-builder';
import { DynamoFilter2 } from './filter';
import { Projection, ProjectionHandler } from './projector';
import { CamelCaseKeys, DynamoConfig, DynamoInfo, TypeFromDefinition } from './types';
import ScanInput = DocumentClient.ScanInput;

export type ScanOptions<INFO extends DynamoInfo, PROJECTION> =
  CamelCaseKeys<Pick<ScanInput, 'Limit' | 'ReturnConsumedCapacity' | 'TotalSegments' | 'Segment' | 'ConsistentRead'>> & {
  projection?: Projection<INFO, PROJECTION>;
  filter?: DynamoFilter2<INFO>;
  next?: string;
}

export type ScanReturn<INFO extends DynamoInfo, PROJECTION> = {
  member: PROJECTION extends null ? TypeFromDefinition<INFO['definition']>[] : PROJECTION[];
  consumedCapacity?: ConsumedCapacity;
  count?: number;
  scannedCount?: number;
  next?: string;
}

export interface ScanExecutor<T extends DynamoInfo, PROJECTION> {
  input: ScanInput;
  execute(): Promise<ScanReturn<T, PROJECTION>>
}

export class DynamoScanner<T extends DynamoInfo> {
  constructor(private readonly info: T, private readonly config: DynamoConfig) {}

  async scan<PROJECTION = null>(options: ScanOptions<T, PROJECTION> = {}): Promise<ScanReturn<T, PROJECTION>> {
    const scanInput = this.scanExecutor(options);
    if (this.config.logStatements) {
      console.log(`ScanInput: ${JSON.stringify(scanInput.input, null, 2)}`);
    }
    return await scanInput.execute();
  }

  scanExecutor<PROJECTION = null>(options: ScanOptions<T, PROJECTION>): ScanExecutor<T, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    const input = {
      TableName: this.config.tableName,
      Limit: options.limit,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ConsistentRead: options.consistentRead,
      TotalSegments: options.totalSegments,
      Segment: options.segment,
      ProjectionExpression: expression,
      ...attributeBuilder.asInput(),
      ...(options.next
        ? {
          ExclusiveStartKey: JSON.parse(
            Buffer.from(options.next, 'base64').toString('ascii'),
          ),
        }
        : {}),
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<ScanReturn<T, PROJECTION>> {
        const result = await client.scan(input).promise();
        return {
          member: (result.Items as any) ?? [],
          consumedCapacity: result.ConsumedCapacity,
          count: result.Count,
          scannedCount: result.ScannedCount,
          next: result.LastEvaluatedKey ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString('base64') : undefined,
        };
      }
    }
  }

  // transactGetExecutor<PROJECTION = null>(keys: PickKeys<T>[], options: GetItemOptions<T, PROJECTION>): TransactGetExecutor<T, PROJECTION> {
  //   const input: TransactGetItemsInput = {
  //     TransactItems: keys.map(key => ({ Get: {Key: key, TableName: this.config.tableName }})),
  //     ReturnConsumedCapacity: options.returnConsumedCapacity
  //   };
  //   const client = this.config.client;
  //   return {
  //     input,
  //     async execute(): Promise<BatchGetItemReturn<T, PROJECTION>> {
  //       const result = await client.transactGet(input).promise();
  //       return {
  //         items: result.Responses?.map(it => it.Item) ?? [] as any,
  //         consumedCapacity: result.ConsumedCapacity?.[0]
  //       }
  //     }
  //   }
  // }
  //
  // async transactGet<PROJECTION = null>(keys: PickKeys<T>[], options: GetItemOptions<T, PROJECTION> = {}): Promise<BatchGetItemReturn<T, PROJECTION>> {
  //   const executor = this.transactGetExecutor(keys, options);
  //   if (this.config.logStatements) {
  //     console.log(`BatchGetItemInput: ${JSON.stringify(executor.input, null, 2)}`);
  //   }
  //   return await executor.execute();
  // }
}