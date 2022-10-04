import { CompareWrapperOperator, Operation, OperationType } from "./operation";
import {
  DynamoEntry,
  DynamoIndexes,
  DynamoMapDefinition,
  SimpleDynamoType
} from "./type-mapping";
import { DynamoFilter2 } from "./filter";
import { AttributeBuilder } from "./attribute-builder";
import { DynamoDefinition } from "./dynamo-client-config";
import { DynamoInfo, DynamoTypeFrom, RawTypeFrom } from "./types";

export type KeyComparisonBuilder<T> = {
  eq(value: T): void;
  lt(value: T): void;
  lte(value: T): void;
  gt(value: T): void;
  gte(value: T): void;
  between(a: T, b: T): void;
  // eslint-disable-next-line @typescript-eslint/ban-types
} & (T extends string ? { beginsWith(value: string): void } : {});

type NestedComparisonBuilder<Original, Type> = {
  exists(): CompareWrapperOperator<Original>;
  notExists(): CompareWrapperOperator<Original>;
  isType(type: SimpleDynamoType): CompareWrapperOperator<Original>;
  beginsWith(beginsWith: string): CompareWrapperOperator<Original>;
  contains(
    operand: Type extends (infer T)[] ? { [K in keyof T]: T[K] } : string
  ): CompareWrapperOperator<Original>;
};


type Digger<T, Original = T> = T extends "///" ? {
    get(key: string): Operation<Original, unknown> & Digger<"///", Original> & NestedComparisonBuilder<Original, unknown>,
    getElement(index: number): Operation<Original, unknown> & Digger<"///", Original> & NestedComparisonBuilder<Original, unknown>
  }
  : T extends { optional: boolean; type: infer Type }
    ? Digger<Type, Original>
    : T extends "map" ? { get(key: string): Operation<Original, unknown> & Digger<"///", Original> & NestedComparisonBuilder<Original, unknown> }
      : T extends "list" ? { getElement(index: number): Operation<Original, unknown> & Digger<"///", Original> & NestedComparisonBuilder<Original, unknown> }
        : T extends { array: infer Type }
          ? {
            getElement(
              index: number
            ): Operation<Original, RawTypeFrom<Type>> &
              Digger<Type, Original> &
              NestedComparisonBuilder<Original, Type>;
          }
          : T extends { object: infer Type }
            ? Digger<Type, Original>
            : T extends Record<string, any>
              ? {
                [K in keyof T]: Operation<Original, RawTypeFrom<T[K]>> &
                Digger<T[K], Original> &
                NestedComparisonBuilder<Original, RawTypeFrom<T[K]>>;
              }
              : {};

export type ComparisonBuilderFrom<INFO extends DynamoInfo> = {
  not(
    comparison: CompareWrapperOperator<Required<DynamoTypeFrom<INFO>>>
  ): CompareWrapperOperator<Required<DynamoTypeFrom<INFO>>>;
  and(
    ...comparisons: CompareWrapperOperator<Required<DynamoTypeFrom<INFO>>>[]
  ): CompareWrapperOperator<Required<DynamoTypeFrom<INFO>>>;
  or(
    ...comparisons: CompareWrapperOperator<Required<DynamoTypeFrom<INFO>>>[]
  ): CompareWrapperOperator<Required<DynamoTypeFrom<INFO>>>;
} & Digger<INFO["definition"]>;

export type ComparisonBuilder<T> = { [K in keyof T]: Operation<T, T[K]> } & {
  exists(key: keyof T): CompareWrapperOperator<T>;
  existsPath(path: string): CompareWrapperOperator<T>;
  notExists(key: keyof T): CompareWrapperOperator<T>;
  notExistsPath(path: string): CompareWrapperOperator<T>;
  isType(key: keyof T, type: SimpleDynamoType): CompareWrapperOperator<T>;
  isTypePath(path: string, type: SimpleDynamoType): CompareWrapperOperator<T>;
  beginsWith(key: keyof T, beginsWith: string): CompareWrapperOperator<T>;
  beginsWithPath(path: string, beginsWith: string): CompareWrapperOperator<T>;
  contains(key: keyof T, operand: string): CompareWrapperOperator<T>;
  containsPath(path: string, operand: string): CompareWrapperOperator<T>;
  not(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
};

export class Wrapper {
  constructor(
    public attributeBuilder: AttributeBuilder,
    public expression: string = ""
  ) {
  }

  add(expression = ""): Wrapper {
    this.expression = expression;
    return this;
  }

  and(comparison: Wrapper): Wrapper {
    this.attributeBuilder.combine(comparison.attributeBuilder);
    this.add(`(${this.expression}) AND (${comparison.expression})`);
    return this;
  }

  or(comparison: Wrapper): Wrapper {
    this.attributeBuilder.combine(comparison.attributeBuilder);
    this.add(`(${this.expression}) OR (${comparison.expression})`);
    return this;
  }
}

export class ComparisonBuilderType<D extends DynamoMapDefinition,
  T extends DynamoEntry<D>,
  > {
  constructor(definition: D, public wrapper: Wrapper) {
    Object.keys(definition).forEach((key) => {
      (this as any)[key] = new OperationType(this.wrapper, definition[key], [
        key
      ]);
    });
  }

  and(...comparisons: Wrapper[]): Wrapper {
    if(comparisons.length > 1) {
      this.wrapper.expression = comparisons.map(comparison => `(${comparison.expression})`).join(" AND ");
    } else if (comparisons.length > 0) {
      this.wrapper.expression = comparisons[0].expression;
    }
    return this.wrapper;
  }

  or(...comparisons: Wrapper[]): Wrapper {
    if(comparisons.length > 1) {
      this.wrapper.expression = comparisons.map(comparison => `(${comparison.expression})`).join(" OR ");
    } else if (comparisons.length > 0) {
      this.wrapper.expression = comparisons[0].expression;
    }
    return this.wrapper;
  }

  not(comparison: Wrapper): Wrapper {
    this.wrapper.expression = `NOT (${comparison.expression})`;
    return this.wrapper;
  }

  builder(): ComparisonBuilder<T> {
    return this as unknown as ComparisonBuilder<T>;
  }
}

export function filterParts<DEFINITION extends DynamoInfo>(
  definition: DEFINITION,
  attributeBuilder: AttributeBuilder,
  filter: DynamoFilter2<DEFINITION>
): string {
  const updatedDefinition = Object.keys(definition.definition)
    .filter((it) => it !== definition.partitionKey && it !== definition.sortKey)
    .reduce((acc, it) => ({ ...acc, [it]: definition.definition[it] }), {});
  const { expression } = filter(
    () =>
      new ComparisonBuilderType(
        updatedDefinition,
        new Wrapper(attributeBuilder)
      ).builder() as any
  ) as unknown as Wrapper;
  return expression;
}

export function conditionalParts<DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null,
  INDEXES extends DynamoIndexes<DEFINITION> = null,
  >(
  definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
  attributeBuilder: AttributeBuilder,
  condition: (
    compare: () => ComparisonBuilder<DynamoEntry<DEFINITION>>
  ) => CompareWrapperOperator<DynamoEntry<DEFINITION>>
): string {
  const updatedDefinition = Object.keys(definition.definition).reduce(
    (acc, it) => ({ ...acc, [it]: definition.definition[it] }),
    {}
  );
  const parent = condition(
    () =>
      new ComparisonBuilderType(
        updatedDefinition,
        new Wrapper(attributeBuilder)
      ).builder() as any
  ) as unknown as Wrapper;
  return parent.expression;
}
