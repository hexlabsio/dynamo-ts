import {
  ConditionGroup,
  ConditionGroupTag,
  Conditions,
  SimpleExpression,
  TupleInferred,
  UnionType,
} from 'src/dynamoTypes';

export class MutableConditionBuilder<T, KS extends (keyof T)[]> {
  private constructor(
    readonly tag: ConditionGroupTag,
    private readonly conditions: Conditions<T, keyof T>[],
  ) {}

  static and<T>(): MutableConditionBuilder<T, (keyof T)[]> {
    return new MutableConditionBuilder<T, (keyof T)[]>('and', []);
  }

  static or<T>(): MutableConditionBuilder<T, (keyof T)[]> {
    return new MutableConditionBuilder<T, (keyof T)[]>('or', []);
  }

  add<C extends keyof T>(
    s: SimpleExpression<T, C> | MutableConditionBuilder<T, C[]>,
  ): MutableConditionBuilder<T, KS> {
    const builder = s as MutableConditionBuilder<T, C[]>;
    if (builder.tag) {
      this.conditions.push(builder.build());
    } else {
      this.conditions.push(s as SimpleExpression<T, C>);
    }
    return this;
  }

  build(): ConditionGroup<SimpleExpression<T, UnionType<KS>>> {
    switch (this.tag) {
      case 'and':
        return { $and: this.conditions };
      case 'or':
        return { $or: this.conditions };
      case 'not':
        return { $not: this.conditions[0] }; //nope!!
    }
  }
}

export class ConditionBuilder<T, KS extends (keyof T)[]> {
  private constructor(
    private readonly operator: ConditionGroupTag,
    private readonly conditions: SimpleExpression<T, keyof T>[],
  ) {}

  static and<T>(): ConditionBuilder<T, TupleInferred<[]>> {
    return new ConditionBuilder<T, TupleInferred<[]>>('and', []);
  }

  static or<T>(): ConditionBuilder<T, TupleInferred<[]>> {
    return new ConditionBuilder<T, TupleInferred<[]>>('or', []);
  }

  add<C extends keyof T>(
    s: SimpleExpression<T, C>,
  ): ConditionBuilder<T, TupleInferred<[C, ...KS]>> {
    return new ConditionBuilder(this.operator, [s, ...this.conditions]);
  }

  build(): ConditionGroup<SimpleExpression<T, UnionType<KS>>> {
    switch (this.operator) {
      case 'and':
        return { $and: this.conditions };
      case 'or':
        return { $or: this.conditions };
      case 'not':
        return { $not: this.conditions[0] };
    }
  }
}
