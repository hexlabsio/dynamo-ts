import {
  ConditionGroup,
  AndOrGroupTag,
  Conditions,
  SimpleExpression,
  GroupTag,
  NotTag,
} from 'src/dynamoTypes';

interface ConditionBuilder<T> {
  tag: GroupTag;
  build(): ConditionGroup<SimpleExpression<T, keyof T>>;
}
class AndOrConditionBuilder<T> implements ConditionBuilder<T> {
  constructor(
    readonly tag: AndOrGroupTag,
    private readonly conditions: Conditions<T, keyof T>[],
  ) {}

  add<C extends keyof T>(
    s: SimpleExpression<T, C> | ConditionBuilder<T>,
  ): AndOrConditionBuilder<T> {
    const builder = s as ConditionBuilder<T>;
    if (builder.tag) {
      this.conditions.push(builder.build());
    } else {
      this.conditions.push(s as SimpleExpression<T, C>);
    }
    return this;
  }

  build(): ConditionGroup<SimpleExpression<T, keyof T>> {
    switch (this.tag) {
      case 'and':
        return { $and: this.conditions };
      case 'or':
        return { $or: this.conditions };
    }
  }
}
class NotConditionBuilder<T> implements ConditionBuilder<T> {
  readonly tag: NotTag = 'not';
  private conditions: Conditions<T, keyof T>;

  add<C extends keyof T>(
    s: SimpleExpression<T, C> | ConditionBuilder<T>,
  ): NotConditionBuilder<T> {
    const builder = s as ConditionBuilder<T>;
    if (builder.tag) {
      this.conditions = builder.build();
    } else {
      this.conditions = s as SimpleExpression<T, C>;
    }
    return this;
  }

  build(): ConditionGroup<SimpleExpression<T, keyof T>> {
    return { $not: this.conditions };
  }
}

export function and<T>(): AndOrConditionBuilder<T> {
  return new AndOrConditionBuilder('and', []);
}

export function or<T>(): AndOrConditionBuilder<T> {
  return new AndOrConditionBuilder('or', []);
}

export function not<T>(): NotConditionBuilder<T> {
  return new NotConditionBuilder();
}
