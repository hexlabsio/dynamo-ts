import { QueryInput } from '@aws-sdk/client-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts, KeyComparisonBuilder, Wrapper } from './comparison';
import { DynamoFilter2 } from './filter';
import { KeyOperation } from './operation';
import { Projection } from './projector';
import { CamelCaseKeys, DynamoConfig, DynamoIndex, DynamoInfo, TypeFromDefinition } from './types';

export type HashCompare<D extends DynamoInfo> = TypeFromDefinition<{ [K in D['partitionKey']]: D['definition'][K] }>
export type SortCompare<D extends DynamoInfo> = D['sortKey'] extends keyof TypeFromDefinition<D['definition']> ? { [K in D['sortKey']]?: (sortKey: KeyComparisonBuilder<TypeFromDefinition<D['definition']>[D['sortKey']]>) => any; } : {};

export type QueryKeys<D extends DynamoInfo> = HashCompare<D> & SortCompare<D>;
export type QuerierInput<D extends DynamoInfo, PROJECTION> = {
  filter?: DynamoFilter2<D>;
  projection?: Projection<D, PROJECTION>;
} & CamelCaseKeys<Pick<QueryInput, 'Limit' | 'ConsistentRead' | 'ScanIndexForward' | 'ReturnConsumedCapacity'>>

export type QuerierReturn<D extends DynamoInfo, PROJECTION> = {
  member: PROJECTION extends null ? TypeFromDefinition<D['definition']>[] : PROJECTION[],
  next?: string;
}

export interface QueryExecutor<D extends DynamoInfo, PROJECTION> {
  input: QueryInput;
  executor: () => Promise<QuerierReturn<D, PROJECTION>>;
}

export class DynamoQuerier<D extends DynamoInfo = any, I extends Record<string, DynamoIndex> = {}> {
  constructor(private readonly info: D, private readonly config: DynamoConfig) {}

  private keyExpression(keys: QueryKeys<D>, attributeBuilder: AttributeBuilder): string {
    const partitionKey = this.info.partitionKey as keyof QueryKeys<D>;
    const sortKey = this.info.sortKey as keyof QueryKeys<D>;
    attributeBuilder.addNames(partitionKey as string);
    const hashValue = keys[partitionKey];
    const valueKey = attributeBuilder.addValue(hashValue);
    const expression = `${attributeBuilder.nameFor(partitionKey as string)} = ${valueKey}`;
    if (sortKey && keys[sortKey]) {
      const keyOperation = new KeyOperation(sortKey as string, new Wrapper(attributeBuilder),);
      (keys[sortKey] as any)(keyOperation);
      return `${expression} AND ${keyOperation.wrapper.expression}`;
    }
    return expression;
  }

  queryExecutor<PROJECTION = null>(keys: QueryKeys<D>, options: QuerierInput<D, PROJECTION>): QueryExecutor<D, PROJECTION> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyExpression(keys, attributeBuilder);
    const filterPart =
      options.filter &&
      filterParts(definition, attributeBuilder, options.filter);
    const projection = ProjectionHandler.projectionExpressionFor(
      attributeBuilder,
      config.definition,
      options.projection,
    );
    const queryInput = {
      TableName: config.tableName,
      ...(config.indexName ? { IndexName: config.indexName } : {}),
      ...{ KeyConditionExpression: keyExpression },
      ...(options.filter ? { FilterExpression: filterPart } : {}),
      ProjectionExpression: projection,
      ...attributeBuilder.asInput(options.dynamo),
      ...(options.next
        ? {
          ExclusiveStartKey: JSON.parse(
            Buffer.from(options.next, 'base64').toString('ascii'),
          ),
        }
        : {}),
    };
  }
}