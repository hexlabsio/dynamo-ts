import {
  DynamoEntry, DynamoIndexes,
  DynamoMapDefinition,
} from "./type-mapping";
import {DynamoClientConfig, DynamoDefinition} from "./dynamo-client-config";
import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import QueryInput = DocumentClient.QueryInput;
import {filterParts, KeyComparisonBuilder, Wrapper} from "./comparison";
import {AttributeBuilder} from "./attribute-builder";
import {KeyOperation} from "./operation";
import {DynamoFilter} from "./filter";
import {Projection, ProjectionHandler} from "./projector";


type HashComparison<HASH extends keyof T, T> = {
  [K in HASH]: T[K]
};

type RangeComparison<R extends keyof T, T> = {
  [K in R]?: (sortKey: KeyComparisonBuilder<T[R]>) => any;
};

type RangeComparisonIfExists<R extends keyof T | null, T> =
    R extends string
        ? RangeComparison<R, T>
        : { }

type Filter<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null
    > = {
  filter?: DynamoFilter<DEFINITION, HASH, RANGE>
}

type ExcessParameters = Omit<
    QueryInput,
    | 'TableName'
    | 'IndexName'
    | 'KeyConditionExpression'
    | 'FilterExpression'
    | 'ExclusiveStartKey'
    >;

export type QueryParametersInput<
    DEFINITION  extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
    PROJECTED = null
    > =
    HashComparison<HASH, DynamoEntry<DEFINITION>> &
    RangeComparisonIfExists<RANGE, DynamoEntry<DEFINITION>> &
    Filter<DEFINITION,HASH, RANGE> &
    {
      projection?: Projection<DEFINITION, PROJECTED>
      next?: string
      dynamo?: ExcessParameters
    }

export type QueryAllParametersInput<
    DEFINITION  extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null
    > =
    QueryParametersInput<DEFINITION, HASH, RANGE> &
    { queryLimit? : number }


export class DynamoQuerier {

  private static keyPart<
      DEFINITION  extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
      INDEXES extends DynamoIndexes<DEFINITION> = null
    >(
      definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
      attributeBuilder: AttributeBuilder,
      queryParameters: HashComparison<HASH, DynamoEntry<DEFINITION>> & RangeComparisonIfExists<RANGE, DynamoEntry<DEFINITION>>
  ): string{
    attributeBuilder.addNames(definition.hash as string)
    const hashValue = queryParameters[definition.hash];
    const valueKey = attributeBuilder.addValue(hashValue);
    const expression = `${attributeBuilder.nameFor(definition.hash as string)}} = ${valueKey}}`;
    if (definition.range && (queryParameters as any)[definition.range]) {
      const keyOperation = new KeyOperation(definition.range as string, new Wrapper(attributeBuilder));
      (queryParameters as any)[definition.range](keyOperation);
      return `${expression} AND ${keyOperation.wrapper.expression}`;
    }
    return expression;
  }

  static async query<
      DEFINITION  extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
      INDEXES extends DynamoIndexes<DEFINITION> = null,
      PROJECTED = null
    >(
      config: DynamoClientConfig<DEFINITION>,
      definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
      options: QueryParametersInput<DEFINITION, HASH, RANGE, PROJECTED>
  ): Promise<{
    next?: string;
    member: { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }[];
  }> {
    const attributeBuilder = AttributeBuilder.create();
    const keyExpression = this.keyPart(definition, attributeBuilder, options);
    const filterPart = options.filter && filterParts(definition, attributeBuilder, options.filter);
    const projection = ProjectionHandler.projectionFor(attributeBuilder, config.definition, options.projection);

    const queryInput: QueryInput = {
      TableName: config.tableName,
      ...(config.indexName ? { IndexName: config.indexName } : {}),
      ...{keyExpression: keyExpression},
      ...(options.filter ? {FilterExpression: filterPart} : {}),
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
    if(config.logStatements) {
      console.log(`QueryInput: ${JSON.stringify(queryInput, null, 2)}`)
    }

    const result = await config.client.query(queryInput).promise();
    return {
      member: (result.Items ?? []) as any[],
      next: result.LastEvaluatedKey
          ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString(
              'base64',
          )
          : undefined,
    } as any;
  }

}