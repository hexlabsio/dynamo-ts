import { DynamoDB } from 'aws-sdk';

type DynamoPrimitive = 'string' | 'number' | 'null' | 'undefined' | 'map' | 'list' | `"${string}"`
export type Definition = DynamoPrimitive
  | `${DynamoPrimitive}?`
  | `${DynamoPrimitive} | ${string}`
  | { optional?: boolean, object: DynamoDefinition }
  | { optional?: boolean, array: Definition };

export type DynamoDefinition = { [key: string]: Definition };

export type Obj<T> = { [K in keyof T]: TypeFrom<T[K]> }

type TypeFrom<S> =
  S extends { object: infer O }
    ? (S extends {optional: infer B} ? {optional: B, type: Obj<O> } : Obj<O>)
    : S extends { array: infer O }
      ? (S extends {optional: infer B} ? {optional: B, type: TypeFrom<O>[] } : TypeFrom<O>[])
      : S extends 'number'
        ? number
        : S extends 'boolean'
          ? boolean
          : S extends 'null'
            ? null
            : S extends 'string'
              ? string
              : S extends 'undefined'
                ? undefined
                : S extends 'map'
                  ? Record<string, any>
                  : S extends 'list'
                    ? any[]
                    : S extends `${infer first} & ${infer rest}`
                      ? TypeFrom<first> & TypeFrom<rest>
                      : S extends `${infer first} | ${infer rest}`
                        ? TypeFrom<first> | TypeFrom<rest>
                        : S extends `"${infer CONST}"`
                          ? CONST
                          : S extends `${infer first}?`
                            ? { optional: true, type: TypeFrom<first> }
                            : never;

type IsOptional<T> = T extends { optional: true } ? true : false
type UndefinedKeys<T> = { [K in keyof T]: IsOptional<T[K]> extends true ? K : never}[keyof T]
type RequiredKeys<T> = { [K in keyof T]: IsOptional<T[K]> extends true ? never : K}[keyof T]
type RequiredParts<T> = { [K in RequiredKeys<T>]: T[K] };
type OptionalParts<T> = { [K in UndefinedKeys<T>]?: T[K] extends { type: infer R } ? R : never };

type MakeOptionalsObject<T> = { [K in keyof T]: T[K] extends (infer A)[] ? MakeOptionals<A>[] : T[K] extends Record<string, any> ? MakeOptionals<T[K]> : T[K]}
type MakeOptionals<T> = RequiredParts<MakeOptionalsObject<T>> & OptionalParts<MakeOptionalsObject<T>>
export type TypeFromDefinition<T> = MakeOptionals<TypeFrom<{ object: T }>>

export type DynamoType<D extends DynamoInfo> = TypeFromDefinition<D['definition']>;

export type DynamoInfo<DEFINITION extends DynamoDefinition = any> = {
  definition: DEFINITION,
  partitionKey: keyof DEFINITION,
  sortKey: keyof DEFINITION | null
}

export type DynamoIndex<DEFINITION extends DynamoDefinition = any> = {
  partitionKey: keyof DEFINITION,
  sortKey?: keyof DEFINITION
}

export interface DynamoConfig {
  logStatements?: boolean;
  tableName: string;
  indexName?: string;
  client: DynamoDB.DocumentClient;
}


export type PickPartition<INFO extends DynamoInfo> = INFO extends { definition: infer DEFINITION; partitionKey: infer KEY }
  ? KEY extends (keyof TypeFromDefinition<DEFINITION>)
    ?  KEY extends string
      ? { [K in KEY]-?: Exclude<TypeFromDefinition<DEFINITION>[KEY], undefined> }
      : never
    : never
  : never;

export type PickSort<INFO extends DynamoInfo> = INFO extends { definition: infer DEFINITION; sortKey: infer KEY }
  ? KEY extends (keyof TypeFromDefinition<DEFINITION>)
    ?  KEY extends string
      ? { [K in KEY]-?: Exclude<TypeFromDefinition<DEFINITION>[KEY], undefined> }
      : {}
    : {}
  : {};

export type PickKeys<INFO extends DynamoInfo> = PickPartition<INFO> & PickSort<INFO>;

export function defineTable<
  DEFINITION extends DynamoDefinition,
  PK extends keyof DEFINITION,
  SK extends Exclude<keyof DEFINITION, PK> | null = null,
  INDEXES extends Record<string, DynamoIndex<DEFINITION>> = {}
  >(
  definition: DEFINITION,
  partitionKey: PK,
  sortKey?: SK,
  indexes?: INDEXES
): {
  definition: DEFINITION,
  partitionKey: PK,
  sortKey: SK,
  indexes: INDEXES
} {
  return {
    definition,
    partitionKey,
    sortKey: sortKey ?? null as any,
    indexes: indexes ?? {} as any
  }
}

export type CamelCaseKey<K> = K extends `${infer F}${infer TAIL}` ? `${Lowercase<F>}${TAIL}` : K;
export type CamelCaseKeys<T> = { [K in keyof T as CamelCaseKey<K>]: T[K] }