import { Wrapper } from './comparison';
import { DynamoType, SimpleDynamoType, TypeFor } from './type-mapping';

export type CompareWrapperOperator<T> = {
  and(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
  or(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
};

export type Operation<T, V> = {
  eq(value: V): CompareWrapperOperator<T>;
  neq(value: V): CompareWrapperOperator<T>;
  lt(value: V): CompareWrapperOperator<T>;
  lte(value: V): CompareWrapperOperator<T>;
  gt(value: V): CompareWrapperOperator<T>;
  gte(value: V): CompareWrapperOperator<T>;
  between(a: V, b: V): CompareWrapperOperator<T>;
  in(b: V[]): CompareWrapperOperator<T>;
};

export class KeyOperation<T> {
  constructor(private readonly key: string, public wrapper: Wrapper) {}

  private add(
    expression: (key: string, value: string) => string,
  ): (value: T) => Wrapper {
    return (value) => {
      const valueKey = this.wrapper.attributeBuilder
        .addNames(this.key)
        .addValue(value);
      const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
      return this.wrapper.add(expression(mappedKey, valueKey));
    };
  }

  eq = this.add((key, value) => `${key} = ${value}`);
  neq = this.add((key, value) => `${key} <> ${value}`);
  lt = this.add((key, value) => `${key} < ${value}`);
  lte = this.add((key, value) => `${key} <= ${value}`);
  gt = this.add((key, value) => `${key} > ${value}`);
  gte = this.add((key, value) => `${key} >= ${value}`);

  between(a: T, b: T): Wrapper {
    this.wrapper.attributeBuilder.addNames(this.key);
    const aKey = this.wrapper.attributeBuilder.addValue(a);
    const bKey = this.wrapper.attributeBuilder.addValue(b);
    const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
    return this.wrapper.add(`${mappedKey} BETWEEN ${aKey} AND ${bKey}`);
  }

  beginsWith(a: T): Wrapper {
    const valueKey = this.wrapper.attributeBuilder
      .addNames(this.key)
      .addValue(a);
    return this.wrapper.add(
      `begins_with(${this.wrapper.attributeBuilder.nameFor(
        this.key,
      )},${valueKey})`,
    );
  }
}

export class OperationType {
  constructor(
    private readonly wrapper: Wrapper,
    readonly subtype: DynamoType,
    readonly parentage: (string | number)[] = [],
  ) {
    if (typeof subtype === 'object') {
      const keys = Object.keys(subtype);
      if (keys.includes('object')) {
        const sub = (subtype as any)['object'];
        Object.keys(sub).forEach((key) => {
          (this as any)[key] = new OperationType(this.wrapper, sub[key], [
            ...parentage,
            key,
          ]);
        });
      } else if (keys.includes('array')) {
        const sub = (subtype as any)['array'];
        (this as any).getElement = (index: number) =>
          new OperationType(this.wrapper, sub, [...parentage, index]);
      }
    }
    if (subtype === 'map' || subtype === 'list') {
      (this as any).getElement = (index: number) =>
        new OperationType(this.wrapper, subtype, [...parentage, index]);
      (this as any).get = (key: string) =>
        new OperationType(this.wrapper, subtype, [...parentage, key]);
    }
  }

  private add(
    expression: (key: string, value: string) => string,
  ): (value: TypeFor<DynamoType>) => CompareWrapperOperator<any> {
    return (value) => {
      const valueKey = this.wrapper.attributeBuilder.addValue(value);
      return this.wrapper.add(expression(this.getKey(), valueKey));
    };
  }
  private getKey(): string {
    this.wrapper.attributeBuilder.addNames(
      ...this.parentage.filter((it): it is string => typeof it === 'string'),
    );
    const names = this.parentage.map((it) =>
      typeof it === 'number'
        ? `[${it}]`
        : this.wrapper.attributeBuilder.nameFor(it),
    );
    return names.join('.').replace(/\.\[/g, '[');
  }

  eq = this.add((key, value) => `${key} = ${value}`);
  neq = this.add((key, value) => `${key} <> ${value}`);
  lt = this.add((key, value) => `${key} < ${value}`);
  lte = this.add((key, value) => `${key} <= ${value}`);
  gt = this.add((key, value) => `${key} > ${value}`);
  gte = this.add((key, value) => `${key} >= ${value}`);

  exists(): Wrapper {
    return this.wrapper.add(`attribute_exists(${this.getKey()})`);
  }

  notExists(): Wrapper {
    return this.wrapper.add(`attribute_not_exists(${this.getKey()})`);
  }

  isType(type: SimpleDynamoType): Wrapper {
    return this.wrapper.add(
      `attribute_type(${this.getKey()}, ${this.wrapper.attributeBuilder.addValue(
        this.typeFor(type),
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

  beginsWith(beginsWith: string): Wrapper {
    return this.wrapper.add(
      `begins_with(${this.getKey()}, ${this.wrapper.attributeBuilder.addValue(
        beginsWith,
      )})`,
    );
  }

  contains(operand: any): Wrapper {
    return this.wrapper.add(
      `contains(${this.getKey()}, ${this.wrapper.attributeBuilder.addValue(
        operand,
      )})`,
    );
  }

  between(
    a: TypeFor<DynamoType>,
    b: TypeFor<DynamoType>,
  ): CompareWrapperOperator<any> {
    const aKey = this.wrapper.attributeBuilder.addValue(a);
    const bKey = this.wrapper.attributeBuilder.addValue(b);
    return this.wrapper.add(`${this.getKey()} BETWEEN ${aKey} AND ${bKey}`);
  }

  in(list: TypeFor<DynamoType>[]): CompareWrapperOperator<any> {
    const valueKeys = list.map((it) =>
      this.wrapper.attributeBuilder.addValue(it),
    );
    return this.wrapper.add(
      `${this.getKey()} IN (${valueKeys.join(',')})`,
    ) as any;
  }
}
