import { CompareWrapperOperator, Operation, OperationType } from './operation';
import {
  DynamoEntry,
  DynamoIndexes,
  DynamoMapDefinition,
  SimpleDynamoType,
} from './type-mapping';
import { DynamoFilter2 } from './filter';
import { AttributeBuilder } from './attribute-builder';
import { DynamoDefinition } from './dynamo-client-config';
import { DynamoInfo, TypeFromDefinition } from './types';

export type KeyComparisonBuilder<T> = {
  eq(value: T): void;
  lt(value: T): void;
  lte(value: T): void;
  gt(value: T): void;
  gte(value: T): void;
  between(a: T, b: T): void;
  // eslint-disable-next-line @typescript-eslint/ban-types
} & (T extends string ? { beginsWith(value: string): void } : {});

type WithoutKeys<T, HASH extends keyof T, RANGE extends keyof T | null> = Omit<T, RANGE extends string ? HASH | RANGE : HASH>;

export type ComparisonBuilderFrom<INFO extends DynamoInfo> = ComparisonBuilder<Required<TypeFromDefinition<WithoutKeys<INFO['definition'], INFO['partitionKey'], INFO['sortKey']>>>>;

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

export class ComparisonBuilderType<
  D extends DynamoMapDefinition,
  T extends DynamoEntry<D>,
> {
  constructor(definition: D, public wrapper: Wrapper) {
    Object.keys(definition).forEach((key) => {
      (this as any)[key] = new OperationType(this.wrapper, key).operation();
    });
  }

  existsPath(path: string): Wrapper {
    return this.wrapper.add(
      `attribute_exists(${this.wrapper.attributeBuilder.buildPath(path)})`,
    );
  }
  exists(key: keyof T): Wrapper {
    this.wrapper.attributeBuilder.addNames(key as string);
    return this.wrapper.add(
      `attribute_exists(${this.wrapper.attributeBuilder.nameFor(
        key as string,
      )})`,
    );
  }
  notExistsPath(path: string): Wrapper {
    return this.wrapper.add(
      `attribute_not_exists(${this.wrapper.attributeBuilder.buildPath(path)})`,
    );
  }
  notExists(key: keyof T): Wrapper {
    this.wrapper.attributeBuilder.addNames(key as string);
    return this.wrapper.add(
      `attribute_not_exists(${this.wrapper.attributeBuilder.nameFor(
        key as string,
      )})`,
    );
  }

  private typeFor(type: SimpleDynamoType): string {
    const withoutOptional = type.endsWith('?')
      ? type.substring(0, type.length - 2)
      : type;
    switch (withoutOptional) {
      case 'string':
        return 'S';
      case 'string set':
        return 'SS';
      case 'number':
        return 'N';
      case 'number set':
        return 'NS';
      case 'binary':
        return 'B';
      case 'binary set':
        return 'BS';
      case 'boolean':
        return 'BOOL';
      case 'null':
        return 'NULL';
      case 'list':
        return 'L';
      default:
        return 'M';
    }
  }

  isType(key: keyof T, type: SimpleDynamoType): Wrapper {
    this.wrapper.attributeBuilder.addNames(key as string);
    return this.wrapper.add(
      `attribute_type(${this.wrapper.attributeBuilder.nameFor(
        key as string,
      )}, ${this.wrapper.attributeBuilder.addValue(this.typeFor(type))})`,
    );
  }
  isTypePath(path: string, type: SimpleDynamoType): Wrapper {
    return this.wrapper.add(
      `attribute_type(${this.wrapper.attributeBuilder.buildPath(
        path,
      )}, ${this.wrapper.attributeBuilder.addValue(this.typeFor(type))})`,
    );
  }

  beginsWith(key: keyof T, beginsWith: string): Wrapper {
    this.wrapper.attributeBuilder.addNames(key as string);
    return this.wrapper.add(
      `begins_with(${this.wrapper.attributeBuilder.nameFor(
        key as string,
      )}, ${this.wrapper.attributeBuilder.addValue(beginsWith)})`,
    );
  }

  beginsWithPath(path: string, beginsWith: string): Wrapper {
    return this.wrapper.add(
      `begins_with(${this.wrapper.attributeBuilder.buildPath(
        path,
      )}, ${this.wrapper.attributeBuilder.addValue(beginsWith)})`,
    );
  }

  contains(key: keyof T, operand: string): Wrapper {
    this.wrapper.attributeBuilder.addNames(key as string);
    return this.wrapper.add(
      `contains(${this.wrapper.attributeBuilder.nameFor(
        key as string,
      )}, ${this.wrapper.attributeBuilder.addValue(operand)})`,
    );
  }

  containsPath(path: string, operand: string): Wrapper {
    return this.wrapper.add(
      `contains(${this.wrapper.attributeBuilder.buildPath(
        path,
      )}, ${this.wrapper.attributeBuilder.addValue(operand)})`,
    );
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
  filter: DynamoFilter2<DEFINITION>,
): string {
  const updatedDefinition = Object.keys(definition.definition)
    .filter((it) => it !== definition.partitionKey && it !== definition.sortKey)
    .reduce((acc, it) => ({ ...acc, [it]: definition.definition[it] }), {});
  const { expression } = filter(
    () => new ComparisonBuilderType(updatedDefinition, new Wrapper(attributeBuilder)).builder() as any,
  ) as unknown as Wrapper;
  return expression;
}

export function conditionalParts<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends keyof DynamoEntry<DEFINITION> | null,
  INDEXES extends DynamoIndexes<DEFINITION> = null,
>(
  definition: DynamoDefinition<DEFINITION, HASH, RANGE, INDEXES>,
  attributeBuilder: AttributeBuilder,
  condition: (
    compare: () => ComparisonBuilder<DynamoEntry<DEFINITION>>,
  ) => CompareWrapperOperator<DynamoEntry<DEFINITION>>,
): string {
  const updatedDefinition = Object.keys(definition.definition).reduce(
    (acc, it) => ({ ...acc, [it]: definition.definition[it] }),
    {},
  );
  const parent = condition(
    () =>
      new ComparisonBuilderType(
        updatedDefinition,
        new Wrapper(attributeBuilder),
      ).builder() as any,
  ) as unknown as Wrapper;
  return parent.expression;
}
