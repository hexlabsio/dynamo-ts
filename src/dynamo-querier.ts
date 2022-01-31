import {
  DynamoEntry, DynamoIndexes,
  DynamoMapDefinition,
} from "./type-mapping";
import {DynamoClientConfig, DynamoDefinition} from "./dynamo-client-config";
import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import QueryInput = DocumentClient.QueryInput;
import {filterParts, KeyComparisonBuilder, Wrapper} from "./comparison";
import {AttributeBuilder} from "./naming";
import {KeyOperation} from "./operation";
import {DynamoFilter} from "./filter";
import {ProjectionHandler} from "./dynamo-getter";


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
    RANGE extends keyof DynamoEntry<DEFINITION> | null = null
    > =
    HashComparison<HASH, DynamoEntry<DEFINITION>> &
    RangeComparisonIfExists<RANGE, DynamoEntry<DEFINITION>> &
    Filter<DEFINITION,HASH, RANGE> &
    {
      projection?: string;
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
  ): [string, AttributeBuilder]{
    const builder = attributeBuilder.addNames(definition.hash as string)
    const hashValue = queryParameters[definition.hash];
    const [valueKey, newBuilder] = builder.addValue(hashValue);
    const expression = `${builder.nameFor(definition.hash as string)}} = ${valueKey}}`;
    if (definition.range && (queryParameters as any)[definition.range]) {
      const keyOperation = new KeyOperation(definition.range as string, new Wrapper(newBuilder));
      (queryParameters as any)[definition.range](keyOperation);
      return [`${expression} AND ${keyOperation.wrapper.expression}`, keyOperation.wrapper.attributeBuilder];
    }
    return [expression, newBuilder];
  }

  static async query<
      DEFINITION  extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null = null,
      INDEXES extends DynamoIndexes<DEFINITION> = null
    >(
      config: DynamoClientConfig<DEFINITION>,
      definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
      attributeBuilder: AttributeBuilder,
      options: QueryParametersInput<DEFINITION, HASH, RANGE>
  ): Promise<{
    next?: string;
    member: { [K in keyof DynamoEntry<DEFINITION>]: DynamoEntry<DEFINITION>[K] }[];
  }> {

    const [keyExpression, builder] = this.keyPart(definition, attributeBuilder, options);
    const filterPart = options.filter && filterParts(definition, builder, options.filter);
    const updatedBuilder = filterPart?.attributeBuilder ?? builder;
    const [updatedBuilder2, projection] = ProjectionHandler.projectionFor(updatedBuilder, config.definition, options.dynamo?.ProjectionExpression);

    const queryInput: QueryInput = {
      TableName: config.tableName,
      ...(config.indexName ? { IndexName: config.indexName } : {}),
      ...{keyExpression: keyExpression},
      ...(options.filter ? {FilterExpression: filterPart!.expression} : {}),
      ProjectionExpression: projection,
      ...updatedBuilder2.asInput(options.dynamo),
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