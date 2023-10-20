import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

type DynamoPrimitive =
  | 'string'
  | 'number'
  | 'null'
  | 'undefined'
  | 'binary'
  | 'binary set'
  | 'string set'
  | 'number set'
  | 'map'
  | 'list'
  | 'boolean'
  | `"${string}"`;
export type Definition =
  | DynamoPrimitive
  | `${DynamoPrimitive}?`
  | `${DynamoPrimitive} | ${string}`
  | { optional?: boolean; object: DynamoDefinition }
  | { optional?: boolean; array: Definition };

export type DynamoDefinition = { [key: string]: Definition };

export type Obj<T> = { [K in keyof T]: TypeFrom<T[K]> };

export type RawUndefinedKeys<T> = {
  [K in keyof T]-?: undefined extends T[K] ? K : never;
}[keyof T];

export type RawRequiredKeys<T> = {
  [K in keyof T]-?: undefined extends T[K] ? never : K;
}[keyof T];

type AddQuestionMarks<T> = {
  [K in RawUndefinedKeys<T>]?: T[K];
} & {
  [K in RawRequiredKeys<T>]: T[K];
};

export type RawObj<T> = AddQuestionMarks<{ [K in keyof T]: RawTypeFrom<T[K]> }>;

export type RawTypeFrom<S> = S extends { object: infer O }
  ? S extends { optional: infer B }
    ? RawObj<O> | undefined
    : RawObj<O>
  : S extends { array: infer O }
  ? RawTypeFrom<O>[]
  : S extends 'number'
  ? number
  : S extends 'number set'
  ? Set<number>
  : S extends 'string set'
  ? Set<string>
  : S extends 'binary'
  ? Uint8Array
  : S extends 'binary set'
  ? Set<Uint8Array>
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
  ? RawTypeFrom<first> & RawTypeFrom<rest>
  : S extends `${infer first} | ${infer rest}`
  ? RawTypeFrom<first> | RawTypeFrom<rest>
  : S extends `"${infer CONST}"`
  ? CONST
  : S extends `${infer first}?`
  ? RawTypeFrom<first> | undefined
  : never;

export type TypeFrom<S> = S extends { object: infer O }
  ? S extends { optional: infer B }
    ? { optional: B; type: Obj<O> }
    : Obj<O>
  : S extends { array: infer O }
  ? S extends { optional: infer B }
    ? { optional: B; type: TypeFrom<O>[] }
    : TypeFrom<O>[]
  : S extends 'number'
  ? number
  : S extends 'number set'
  ? Set<number>
  : S extends 'string set'
  ? Set<string>
  : S extends 'binary'
  ? Uint8Array
  : S extends 'binary set'
  ? Set<Uint8Array>
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
  ? { optional: true; type: TypeFrom<first> }
  : never;

type IsOptional<T> = T extends { optional: true } ? true : false;
type OptionalKeys<T> = {
  [K in keyof T]: IsOptional<T[K]> extends true ? K : never;
}[keyof T];
type RequiredKeys<T> = {
  [K in keyof T]: IsOptional<T[K]> extends true ? never : K;
}[keyof T];

type RequiredParts<T> = {
  [K in RequiredKeys<T>]: T[K] extends { optional: false; type: infer R }
    ? R
    : T[K];
};

type OptionalParts<T> = {
  [K in OptionalKeys<T>]?: T[K] extends { type: infer R } ? R : never;
};

type MakeOptionalsObject<T> = {
  [K in keyof T]: T[K] extends (infer A)[]
    ? MakeOptionals<A>[]
    : T[K] extends Record<string, any>
    ? MakeOptionals<T[K]>
    : T[K];
};

type MakeOptionals<T> = T extends Set<any>
  ? T
  : T extends Uint8Array
  ? T
  : T extends Record<string | number, any>
  ? RequiredParts<MakeOptionalsObject<T>> &
      OptionalParts<MakeOptionalsObject<T>>
  : T;

export type TypeFromDefinition<T> = MakeOptionals<TypeFrom<{ object: T }>>;

export type Expand<T> = T extends Set<any>
  ? T
  : T extends Uint8Array
  ? T
  : T extends {}
  ? { [K in keyof T]: Expand<T[K]> }
  : T;

export type DynamoTypeFrom<D extends DynamoInfo> = Expand<{
  [K in keyof TypeFromDefinition<D['definition']>]: TypeFromDefinition<
    D['definition']
  >[K];
}>;

export type DynamoIndex<DEFINITION extends DynamoDefinition = any> = {
  partitionKey: keyof DEFINITION;
  sortKey?: keyof DEFINITION;
};

export type DynamoInfo<
  DEFINITION extends DynamoDefinition = any,
  I extends Record<string, DynamoIndex<DEFINITION>> = any,
> = {
  definition: DEFINITION;
  partitionKey: keyof DEFINITION;
  sortKey: keyof DEFINITION | null;
  indexes: I;
};

export interface DynamoConfig {
  logStatements?: boolean;
  tableName: string;
  indexName?: string;
  client: DynamoDBDocument;
}

export type PickPartition<INFO extends DynamoInfo> = INFO extends {
  definition: infer DEFINITION;
  partitionKey: infer KEY;
}
  ? KEY extends keyof TypeFromDefinition<DEFINITION>
    ? KEY extends string
      ? {
          [K in KEY]-?: Exclude<TypeFromDefinition<DEFINITION>[KEY], undefined>;
        }
      : never
    : never
  : never;

export type PickSort<INFO extends DynamoInfo> = INFO extends {
  definition: infer DEFINITION;
  sortKey: infer KEY;
}
  ? KEY extends keyof TypeFromDefinition<DEFINITION>
    ? KEY extends string
      ? {
          [K in KEY]-?: Exclude<TypeFromDefinition<DEFINITION>[KEY], undefined>;
        }
      : {}
    : {}
  : {};

export type PickKeys<INFO extends DynamoInfo> = PickPartition<INFO> &
  PickSort<INFO>;

export function defineTable<
  DEFINITION extends DynamoDefinition,
  PK extends keyof DEFINITION,
  SK extends Exclude<keyof DEFINITION, PK> | null = null,
  INDEXES extends Record<string, DynamoIndex<DEFINITION>> = {},
>(
  definition: DEFINITION,
  partitionKey: PK,
  sortKey?: SK,
  indexes?: INDEXES,
): {
  definition: DEFINITION;
  partitionKey: PK;
  sortKey: SK;
  indexes: INDEXES;
} {
  return {
    definition,
    partitionKey,
    sortKey: sortKey ?? (null as any),
    indexes: indexes ?? ({} as any),
  };
}

export type CamelCaseKey<K> = K extends `${infer F}${infer TAIL}`
  ? `${Lowercase<F>}${TAIL}`
  : K;
export type CamelCaseKeys<T> = { [K in keyof T as CamelCaseKey<K>]: T[K] };
