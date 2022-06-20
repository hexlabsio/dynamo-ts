import { DynamoClientConfig } from './dynamo-client-config';

// prettier-ignore
export type NestedDepthTable = {
  32: 31; 31: 30; 30: 29; 29: 28; 28: 27; 27: 26; 26: 25; 25: 24; 24: 23;
  23: 22; 22: 21; 21: 20; 20: 19; 19: 18; 18: 17; 17: 16; 16: 15; 15: 14;
  14: 13; 13: 12; 12: 11; 11: 10; 10: 9; 9: 8; 8: 7; 7: 6; 6: 5; 5: 4;
  4: 3; 3: 2; 2: 1; 1: 0; 0: never;
};

export type NestedDepthKey = keyof NestedDepthTable;
export type NestedDepthValue = NestedDepthTable[NestedDepthKey];
export type NestedDepthElem = NestedDepthKey | NestedDepthValue;
export type Dec<T extends number> = T extends NestedDepthKey
  ? NestedDepthTable[T]
  : never;

export type PathKeys<T, Depth extends NestedDepthElem = 32> = PathKeysRec<
  T,
  '',
  never,
  Depth
>;
type PathKeysRec<
  T,
  Path extends string,
  Acc extends string,
  Depth extends NestedDepthElem,
> = Depth extends never
  ? never
  : T extends (infer X)[]
  ? PathKeysRec<
      X,
      `${DotSuffix<Path>}[${number}]`,
      Acc | `${DotSuffix<Path>}[${number}]`,
      Dec<Depth>
    >
  : T extends object
  ? SubKeys<T, keyof T, Path, Acc, Dec<Depth>>
  : Acc;

type SubKeys<
  T,
  K extends keyof T,
  Path extends string,
  Acc extends string,
  Depth extends NestedDepthElem,
> = K extends infer S
  ? S extends string
    ? PathKeysRec<
        T[K],
        `${DotSuffix<Path>}${S}`,
        Acc | `${DotSuffix<Path>}${S}`,
        Depth
      >
    : Acc
  : Acc;

type DotSuffix<T extends string> = T extends '' ? '' : `${T}.`;

type PathValue<P extends string, T> = T extends (infer X)[]
  ? P extends `[${number}].${infer TAIL}`
    ? PathValue<TAIL, X>
    : P extends `[${number}]`
    ? X
    : never
  : T extends object
  ? P extends `${infer A}.${infer TAIL}`
    ? A extends keyof T
      ? PathValue<TAIL, Required<T>[A]>
      : never
    : P extends keyof T
    ? Required<T>[P] | null
    : never
  : never;

export type DynamoNestedKV<DEFINITION> = Partial<{
  [K in PathKeys<DEFINITION>]: PathValue<K, DEFINITION>;
}>;

export type DynamoIndexes<DEFINITION extends DynamoMapDefinition> = Record<
  string,
  {
    local?: boolean;
    hashKey: keyof DynamoEntry<DEFINITION>;
    rangeKey: keyof DynamoEntry<DEFINITION> | null;
  }
> | null;

export type DynamoIndexBaseKeys<DEFINITION extends DynamoMapDefinition> = {
  hash: keyof DynamoEntry<DEFINITION>;
  range?: keyof DynamoEntry<DEFINITION> | null;
} | null;

export type DynamoMapDefinition = { [key: string]: DynamoType };
export type DynamoType = SimpleDynamoType | DynamoEntryDefinition;
export type DynamoObjectDefinition = {
  optional?: boolean;
  object: DynamoMapDefinition;
};
export type DynamoArrayDefinition = { optional?: boolean; array: DynamoType };
export type DynamoEntryDefinition =
  | DynamoObjectDefinition
  | DynamoArrayDefinition;

type UndefinedKeys<T> = {
  [P in keyof T]: undefined extends T[P] ? P : never;
}[keyof T];
type PartializeTop<T> = Partial<Pick<T, UndefinedKeys<T>>> &
  Omit<T, UndefinedKeys<T>>;
type PartializeObj<T> = {
  [K in keyof T]: T[K] extends Record<string, unknown>
    ? Partialize<T[K]>
    : T[K] extends (infer A)[]
    ? (A extends Record<string, unknown> ? Partialize<A> : A)[]
    : T[K];
};
type Partialize<T> = PartializeObj<PartializeTop<T>>;
export type DynamoEntry<T extends DynamoObjectDefinition['object']> =
  Partialize<{
    [K in keyof T]: TypeFor<T[K]>;
  }>;
export type IndexDefinition<T extends DynamoEntry<any>> = {
  hashKey: keyof T;
  rangeKey?: keyof T;
};

export type DynamoAnyEntry<
  T extends DynamoArrayDefinition['array'] | DynamoObjectDefinition['object'],
> = T extends DynamoObjectDefinition['object']
  ? {
      [K in keyof T]: TypeFor<T[K]>;
    }
  : T extends DynamoArrayDefinition['array']
  ? TypeFor<T>[]
  : never;

export type TypeFor<T extends DynamoType> = T extends 'string'
  ? string
  : T extends 'string set'
  ? string[]
  : T extends 'number'
  ? number
  : T extends 'number set'
  ? number[]
  : T extends 'binary'
  ? Buffer
  : T extends 'binary set'
  ? Buffer[]
  : T extends 'list'
  ? unknown[]
  : T extends 'map'
  ? Record<string, unknown>
  : T extends 'boolean'
  ? boolean
  : T extends 'null'
  ? null
  : T extends 'string?'
  ? string | undefined
  : T extends 'string set?'
  ? string[] | undefined
  : T extends 'number?'
  ? number | undefined
  : T extends 'number set?'
  ? number[] | undefined
  : T extends 'binary?'
  ? Buffer | undefined
  : T extends 'binary set?'
  ? Buffer[] | undefined
  : T extends 'list?'
  ? unknown[] | undefined
  : T extends 'map?'
  ? Record<string, unknown> | undefined
  : T extends 'boolean?'
  ? boolean | undefined
  : T extends DynamoEntryDefinition
  ? T['optional'] extends true
    ?
        | (T extends { object: any }
            ? { [K in keyof T['object']]: DynamoAnyEntry<T['object']>[K] }
            : T extends { array: any }
            ? DynamoAnyEntry<T['array']>
            : never)
        | undefined
    : T extends { object: any }
    ? { [K in keyof T['object']]: DynamoAnyEntry<T['object']>[K] }
    : T extends { array: any }
    ? DynamoAnyEntry<T['array']>
    : never
  : never;

export type SimpleDynamoType =
  | 'string'
  | 'string set'
  | 'number'
  | 'number set'
  | 'binary set'
  | 'binary'
  | 'boolean'
  | 'null'
  | 'list'
  | 'map'
  | 'string?'
  | 'string set?'
  | 'number?'
  | 'number set?'
  | 'binary set?'
  | 'binary?'
  | 'boolean?'
  | 'list?'
  | 'map?';

export type DynamoRangeKey<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoClientConfig<DEFINITION>['tableType'],
> = Omit<keyof DynamoClientConfig<DEFINITION>['tableType'], HASH> | null;

export type DynamoKeysFrom<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoClientConfig<DEFINITION>['tableType'],
  RANGE extends DynamoRangeKey<DEFINITION, HASH>,
> = RANGE extends string
  ? RANGE extends keyof DynamoClientConfig<DEFINITION>['tableType']
    ? { [K in HASH | RANGE]: DynamoClientConfig<DEFINITION>['tableType'][K] }
    : never
  : { [K in HASH]: DynamoClientConfig<DEFINITION>['tableType'][K] };

export type DynamoNonKeysFrom<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoClientConfig<DEFINITION>['tableType'],
  RANGE extends DynamoRangeKey<DEFINITION, HASH>,
> = RANGE extends string
  ? RANGE extends keyof DynamoClientConfig<DEFINITION>['tableType']
    ? Omit<DynamoClientConfig<DEFINITION>['tableType'], HASH | RANGE>
    : never
  : Omit<DynamoClientConfig<DEFINITION>['tableType'], HASH>;
