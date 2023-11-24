type JsonPathRecursive<
  T,
  Path extends string = '',
  Acc extends string = never,
  Depth extends 0[] = [],
> =
  Depth['length'] extends 32
  ? never
  : T extends (infer X)[]
    ? JsonPathRecursive<
      X,
      `${DotSuffix<Path>}[${number}]`,
      Acc | `${DotSuffix<Path>}[${number}]`,
      [...Depth, 0]
    >
    : T extends object
      ? SubKeys<T, keyof T, Path, Acc, [...Depth, 0]>
      : Acc;

type SubKeys<
  T,
  K extends keyof T,
  Path extends string,
  Acc extends string,
  Depth extends 0[],
> = K extends infer S
  ? S extends string
    ? JsonPathRecursive<
      T[K],
      `${DotSuffix<Path>}${S}`,
      Acc | `${DotSuffix<Path>}${S}`,
      Depth
    >
    : Acc
  : Acc;

type DotSuffix<T extends string> = T extends '' ? '' : `${T}.`;


export type ValueAtJsonPath<P extends string, T> =
  T extends (infer X)[]
  ? P extends `[${number}].${infer TAIL}`
    ? ValueAtJsonPath<TAIL, X>
    : P extends `[${number}]`
      ? X
      : never
  : T extends object
    ? P extends `${infer A}.${infer TAIL}`
      ? A extends keyof T
        ? ValueAtJsonPath<TAIL, Required<T>[A]>
        : never
      : P extends keyof T
        ? Required<T>[P]
        : never
    : never;

export type JsonPath<T> = JsonPathRecursive<T>;
