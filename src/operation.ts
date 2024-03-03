import { Wrapper } from './comparison.js';
import { SimpleDynamoType } from './table-builder/table-definition.js';

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

function getKey(
  wrapper: Wrapper,
  parentage: (string | number | symbol)[] = [],
): string {
  wrapper.attributeBuilder.addNames(
    ...parentage.filter((it): it is string => typeof it === 'string'),
  );
  const names = parentage.map((it) =>
    typeof it === 'number' ? `[${it}]` : wrapper.attributeBuilder.nameFor(it),
  );
  return names.join('.').replace(/\.\[/g, '[');
}

export function operationProxy(
  wrapper: Wrapper,
  parentage: (string | number | symbol)[] = [],
): any {
  return new Proxy(
    {},
    {
      get(target, name, ...rest) {
        if (name === 'notExists') {
          return wrapper.add(
            `attribute_not_exists(${getKey(wrapper, parentage)})`,
          );
        }
        if (name === 'exists') {
          return wrapper.add(`attribute_exists(${getKey(wrapper, parentage)})`);
        }
        if (
          Object.getOwnPropertyNames(OperationType.prototype).includes(
            name.toString(),
          )
        ) {
          const fn = new OperationType(wrapper, parentage);
          return (fn as any)[name].bind(fn);
        }
        const isNumber = !isNaN(name as any);
        return operationProxy(wrapper, [
          ...parentage,
          isNumber ? +name.toString() : name,
        ]);
      },
    },
  );
}

class OperationType {
  constructor(
    private readonly wrapper: Wrapper,
    readonly parentage: (string | number | symbol)[] = [],
  ) {}
  private add(
    expression: (key: string, value: string) => string,
  ): (value: any) => CompareWrapperOperator<any> {
    return (value) => {
      const valueKey = this.wrapper.attributeBuilder.addValue(value);
      return this.wrapper.add(
        expression(getKey(this.wrapper, this.parentage), valueKey),
      );
    };
  }

  eq(value: any) {
    return this.add((key, value) => `${key} = ${value}`)(value);
  }
  neq(value: any) {
    return this.add((key, value) => `${key} <> ${value}`)(value);
  }
  lt(value: any) {
    return this.add((key, value) => `${key} < ${value}`)(value);
  }
  lte(value: any) {
    return this.add((key, value) => `${key} <= ${value}`)(value);
  }
  gt(value: any) {
    return this.add((key, value) => `${key} > ${value}`)(value);
  }
  gte(value: any) {
    return this.add((key, value) => `${key} >= ${value}`)(value);
  }

  isType(type: SimpleDynamoType): Wrapper {
    return this.wrapper.add(
      `attribute_type(${getKey(
        this.wrapper,
        this.parentage,
      )}, ${this.wrapper.attributeBuilder.addValue(this.typeFor(type))})`,
    );
  }

  private typeFor(type: SimpleDynamoType): string {
    switch (type) {
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
      `begins_with(${getKey(
        this.wrapper,
        this.parentage,
      )}, ${this.wrapper.attributeBuilder.addValue(beginsWith)})`,
    );
  }

  contains(operand: any): Wrapper {
    return this.wrapper.add(
      `contains(${getKey(
        this.wrapper,
        this.parentage,
      )}, ${this.wrapper.attributeBuilder.addValue(operand)})`,
    );
  }

  between(a: any, b: any): CompareWrapperOperator<any> {
    const aKey = this.wrapper.attributeBuilder.addValue(a);
    const bKey = this.wrapper.attributeBuilder.addValue(b);
    return this.wrapper.add(
      `${getKey(this.wrapper, this.parentage)} BETWEEN ${aKey} AND ${bKey}`,
    );
  }

  in(list: any[]): CompareWrapperOperator<any> {
    const valueKeys = list.map((it) =>
      this.wrapper.attributeBuilder.addValue(it),
    );
    return this.wrapper.add(
      `${getKey(this.wrapper, this.parentage)} IN (${valueKeys.join(',')})`,
    ) as any;
  }
}
