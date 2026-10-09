import { AttributeBuilder } from './attribute-builder.js';
import { JsonPath } from './types/index.js';

/**
 * The attribute paths to read, e.g. `['model', 'specs.seats', 'tags.[0]']`. P is the selected paths, inferred from the
 * argument.
 */
export type Select<TableType, P> = P & readonly JsonPath<TableType>[];

type BuiltIn = Uint8Array | Set<any> | Map<any, any> | Date;

type Simplify<T> = T extends BuiltIn
  ? T
  : T extends readonly (infer E)[]
  ? Simplify<E>[]
  : T extends object
  ? { [K in keyof T]: Simplify<T[K]> }
  : T;

type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (
  x: infer I,
) => void
  ? I
  : never;

type ListOf<T> = NonNullable<T> extends (infer E)[] ? E : never;

// Keeps an attribute optional if it's optional in the table type
type Field<T, K extends keyof T, V> = {} extends Pick<T, K>
  ? { [Key in K]?: V }
  : { [Key in K]: V };

// What a list element path (after the list attribute) reads, e.g. `[0]` or `[0].name`
type ElementPath<
  E,
  After extends string,
> = After extends `.[${number}]${infer Next}`
  ? ElementPath<ListOf<E>, Next>[]
  : After extends `.${infer Tail}`
  ? PathObject<NonNullable<E>, Tail>
  : E;

// The part of an item one path reads. Paths that aren't in T read nothing.
type PathObject<T, P extends string> = P extends `${infer K}.${infer Rest}`
  ? K extends keyof T
    ? Rest extends `[${number}]${infer After}`
      ? { [Key in K]?: ElementPath<ListOf<T[K]>, After>[] }
      : Field<T, K, PathObject<NonNullable<T[K]>, Rest>>
    : {}
  : P extends keyof T
  ? Pick<T, P>
  : {};

/**
 * The type of an item read with the selected paths.
 */
export type Selected<T, P extends readonly string[]> = Simplify<
  UnionToIntersection<PathObject<T, P[number]>>
>;

/**
 * The item type, or the selected part of it when paths are selected.
 */
export type Projected<T, P> = P extends readonly string[] ? Selected<T, P> : T;

/**
 * Removes duplicate paths and paths inside another selected path, which DynamoDB rejects as overlapping.
 */
export function selectPaths(paths: readonly string[]): string[] {
  const unique = [...new Set(paths)];
  return unique.filter(
    (path) =>
      !unique.some((other) => other !== path && path.startsWith(`${other}.`)),
  );
}

/**
 * The projection expression for the selected paths.
 */
export function selectExpression(
  attributeBuilder: AttributeBuilder,
  paths: readonly string[],
): string {
  return selectPaths(paths)
    .map((path) => attributeBuilder.buildPath(path))
    .join(',');
}
