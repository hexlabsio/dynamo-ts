import { ScanCommandInput, ScanCommandOutput } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { Projection, ProjectionHandler } from './projector';
import { TableDefinition } from './table-builder/table-definition';
import { CamelCaseKeys } from './types/camel-case';
import { DynamoConfig } from './types/dynamo-config';
import { DynamoFilter } from './types/filter';

export type ScanOptions<TableType, PROJECTION> = CamelCaseKeys<
  Pick<
    ScanCommandInput,
    | 'Limit'
    | 'ReturnConsumedCapacity'
    | 'TotalSegments'
    | 'Segment'
    | 'ConsistentRead'
  >
> & {
  projection?: Projection<TableType, PROJECTION>;
  filter?: DynamoFilter<TableType>;
  next?: string;
};

export type ScanReturn<TableType, PROJECTION> = {
  member: PROJECTION extends null ? TableType[] : PROJECTION[];
  consumedCapacity?: ScanCommandOutput['ConsumedCapacity'];
  count?: number;
  scannedCount?: number;
  next?: string;
};

export interface ScanExecutor<TableType, PROJECTION> {
  input: ScanCommandInput;
  execute(): Promise<ScanReturn<TableType, PROJECTION>>;
}

export class DynamoScanner<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  async scan<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<ScanReturn<TableConfig['type'], PROJECTION>> {
    const scanInput = this.scanExecutor(options);
    if (this.clientConfig.logStatements) {
      console.log(`ScanInput: ${JSON.stringify(scanInput.input, null, 2)}`);
    }
    return await scanInput.execute();
  }

  async scanAll<PROJECTION = null>(
    options: ScanOptions<TableConfig['type'], PROJECTION> = {},
  ): Promise<Omit<ScanReturn<TableConfig['type'], PROJECTION>, 'next'>> {
    const executor = this.scanExecutor(options);
    if (this.clientConfig.logStatements) {
      console.log(`ScanInput: ${JSON.stringify(executor.input, null, 2)}`);
    }
    let result = await executor.execute();
    let scannedCount = result.scannedCount ?? 0;
    const member = result.member;
    while (result.next) {
      executor.input.ExclusiveStartKey = JSON.parse(
        Buffer.from(result.next, 'base64').toString(),
      );
      if (this.clientConfig.logStatements) {
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
    options: ScanOptions<TableConfig['type'], PROJECTION>,
  ): ScanExecutor<TableConfig['type'], PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const expression =
      options.projection &&
      ProjectionHandler.projectionExpressionFor(
        attributeBuilder,
        options.projection,
      );
    const filterPart =
      options.filter && filterParts(attributeBuilder, options.filter);
    const input = {
      TableName: this.clientConfig.tableName,
      ...(this.clientConfig.indexName
        ? { IndexName: this.clientConfig.indexName }
        : {}),
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
    const client = this.clientConfig.client;
    return {
      input,
      async execute(): Promise<ScanReturn<TableConfig['type'], PROJECTION>> {
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
