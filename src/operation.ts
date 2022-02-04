import { Wrapper } from './comparison';
import { DynamoType, TypeFor } from './type-mapping';

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
    return this.wrapper.add(`${mappedKey} BETWEEN ${aKey} AND :${bKey}`);
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
    private readonly key: string,
  ) {}
  operation(): Operation<any, any> {
    return this as unknown as Operation<any, any>;
  }

  private add(
    expression: (key: string, value: string) => string,
  ): (value: TypeFor<DynamoType>) => CompareWrapperOperator<any> {
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

  between(
    a: TypeFor<DynamoType>,
    b: TypeFor<DynamoType>,
  ): CompareWrapperOperator<any> {
    this.wrapper.attributeBuilder.addNames(this.key);
    const aKey = this.wrapper.attributeBuilder.addValue(a);
    const bKey = this.wrapper.attributeBuilder.addValue(b);
    const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
    return this.wrapper.add(`${mappedKey} BETWEEN ${aKey} AND :${bKey}`);
  }

  in(list: TypeFor<DynamoType>[]): CompareWrapperOperator<any> {
    this.wrapper.attributeBuilder.addNames(this.key);
    const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
    const valueKeys = list.map((it) =>
      this.wrapper.attributeBuilder.addValue(it),
    );
    return this.wrapper.add(`${mappedKey} IN (${valueKeys.join(',')})`) as any;
  }
}
