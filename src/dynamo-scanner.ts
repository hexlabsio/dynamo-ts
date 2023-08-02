import { ScanCommandInput, ScanCommandOutput } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { DynamoFilter } from './filter';
import { Projection, ProjectionHandler } from './projector';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoInfo,
  TypeFromDefinition,
} from './types';

export type ScanOptions<INFO extends DynamoInfo, PROJECTION> = CamelCaseKeys<
  Pick<
    ScanCommandInput,
    | 'Limit'
    | 'ReturnConsumedCapacity'
    | 'TotalSegments'
    | 'Segment'
    | 'ConsistentRead'
  >
> & {
  projection?: Projection<INFO, PROJECTION>;
  filter?: DynamoFilter<INFO>;
  next?: string;
};

export type ScanReturn<INFO extends DynamoInfo, PROJECTION> = {
  member: PROJECTION extends null
    ? TypeFromDefinition<INFO['definition']>[]
    : PROJECTION[];
  consumedCapacity?: ScanCommandOutput['ConsumedCapacity'];
  count?: number;
  scannedCount?: number;
  next?: string;
};

export interface ScanExecutor<T extends DynamoInfo, PROJECTION> {
  input: ScanCommandInput;
  execute(): Promise<ScanReturn<T, PROJECTION>>;
}

export class DynamoScanner<T extends DynamoInfo> {
  constructor(
    private readonly info: T,
    private readonly config: DynamoConfig,
  ) {}

  async scan<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION> = {},
  ): Promise<ScanReturn<T, PROJECTION>> {
    const scanInput = this.scanExecutor(options);
    if (this.config.logStatements) {
      console.log(`ScanInput: ${JSON.stringify(scanInput.input, null, 2)}`);
    }
    return await scanInput.execute();
  }

  async scanAll<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION> = {},
  ): Promise<Omit<ScanReturn<T, PROJECTION>, 'next'>> {
    const executor = this.scanExecutor(options);
    if (this.config.logStatements) {
      console.log(`ScanInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    let result = await executor.execute();
    let scannedCount = result.scannedCount ?? 0;
    const member = result.member;
    while (result.next) {
      executor.input.ExclusiveStartKey = JSON.parse(
        Buffer.from(result.next, 'base64').toString(),
      );
      if (this.config.logStatements) {
        console.log(`ScanInput: ${JSON.stringify(executor.input, null, 2)}`);
      }
      result = await executor.execute();
      member.push(...(result.member as any[]));
      scannedCount = scannedCount + (result.scannedCount ?? 0);
    }
    return {
      member,
      count: member.length,
      scannedCount,
      consumedCapacity: result.consumedCapacity,
    };
  }

  scanExecutor<PROJECTION = null>(
    options: ScanOptions<T, PROJECTION>,
  ): ScanExecutor<T, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      this.info,
      options.projection,
    );
    const filterPart =
      options.filter &&
      filterParts(this.info, attributeBuilder, options.filter);
    const input = {
      TableName: this.config.tableName,
      ...(this.config.indexName ? { IndexName: this.config.indexName } : {}),
      Limit: options.limit,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ConsistentRead: options.consistentRead,
      TotalSegments: options.totalSegments,
      Segment: options.segment,
      ProjectionExpression: expression,
      ...(options.filter ? { FilterExpression: filterPart } : {}),
      ...attributeBuilder.asInput(),
      ...(options.next
        ? {
            ExclusiveStartKey: JSON.parse(
              Buffer.from(options.next, 'base64').toString(),
            ),
          }
        : {}),
    };
    const client = this.config.client;
    return {
      input,
      async execute(): Promise<ScanReturn<T, PROJECTION>> {
        const result = await client.scan(input);
        return {
          member: (result.Items as any) ?? [],
          consumedCapacity: result.ConsumedCapacity,
          count: result.Count,
          scannedCount: result.ScannedCount,
          next: result.LastEvaluatedKey
            ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
                'base64',
              )
            : undefined,
        };
      },
    };
  }
}
