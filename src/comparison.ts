import { CompareWrapperOperator, Operation, operationProxy } from './operation';

import { DynamoFilter } from './types';
import { AttributeBuilder } from './attribute-builder';
import { SimpleDynamoType } from './table-builder/table-definition';

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
  exists: CompareWrapperOperator<Original>;
  notExists: CompareWrapperOperator<Original>;
  isType(type: SimpleDynamoType): CompareWrapperOperator<Original>;
  beginsWith(beginsWith: string): CompareWrapperOperator<Original>;
  contains(
    operand: Type extends (infer T)[] ? { [K in keyof T]: T[K] } : string,
  ): CompareWrapperOperator<Original>;
};

type Digger<T, Original = T> = Required<{
  [K in keyof T]: Operation<T, T[K]> & Digger<T[K], Original> & NestedComparisonBuilder<Original, T[K]>;
}>

export type ComparisonBuilderFrom<TableType> = {
  not(
    comparison: CompareWrapperOperator<Required<TableType>>,
  ): CompareWrapperOperator<Required<TableType>>;
  and(
    ...comparisons: CompareWrapperOperator<Required<TableType>>[]
  ): CompareWrapperOperator<Required<TableType>>;
  or(
    ...comparisons: CompareWrapperOperator<Required<TableType>>[]
  ): CompareWrapperOperator<Required<TableType>>;
} & Digger<TableType>;

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
    public expression: string = '',
  ) {}

  add(expression = ''): Wrapper {
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

export function comparisonBuilderProxy(wrapper: Wrapper) {
  return new Proxy({}, {
    get(target, name) {
      if(Object.getOwnPropertyNames(ComparisonBuilderType.prototype).includes(name.toString())) {
        const builder =new ComparisonBuilderType(wrapper);
        return (builder as any)[name].bind(builder);
      }
      return operationProxy(wrapper, [name]);
    }
  })
}


class ComparisonBuilderType<T>{
  constructor(public wrapper: Wrapper) {}

  and(...comparisons: Wrapper[]): Wrapper {
    if (comparisons.length > 1) {
      this.wrapper.expression = comparisons
        .map((comparison) => `(${comparison.expression})`)
        .join(' AND ');
    } else if (comparisons.length > 0) {
      this.wrapper.expression = comparisons[0].expression;
    }
    return this.wrapper;
  }

  or(...comparisons: Wrapper[]): Wrapper {
    if (comparisons.length > 1) {
      this.wrapper.expression = comparisons
        .map((comparison) => `(${comparison.expression})`)
        .join(' OR ');
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

export function filterParts<TableType>(
  attributeBuilder: AttributeBuilder,
  filter: DynamoFilter<TableType>,
): string {
  const { expression } = filter(() => comparisonBuilderProxy(new Wrapper(attributeBuilder)) as any) as any;
  return expression;
}

export function conditionalParts<TableType>(
  attributeBuilder: AttributeBuilder,
  condition: (
    compare: () => ComparisonBuilder<TableType>,
  ) => CompareWrapperOperator<TableType>,
): string {
  const parent = condition(
    () =>
      new ComparisonBuilderType(
        new Wrapper(attributeBuilder),
      ).builder() as any,
  ) as unknown as Wrapper;
  return parent.expression;
}
