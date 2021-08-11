import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import IndexName = DocumentClient.IndexName;
import Key = DocumentClient.Key;

export type Operator =
  | SingleOperator
  | RangeOperator
  | ArrayOperator
  | AttrCheckOperator;
export type TupleType<T extends readonly any[]> = T extends readonly [
  infer HEAD,
]
  ? HEAD
  : T extends readonly [infer HEAD, ...infer TAIL]
  ? HEAD | TupleType<TAIL>
  : never;

export const attributeCheckValues = [
  'attribute_exists',
  'attribute_not_exists',
] as const;
export const rangeOperatorValues = ['between'] as const;
export const arrayOperatorValues = ['in'] as const;
export const keySingleOperatorValues = [
  '=',
  '<=',
  '<',
  '>=',
  '>',
  'begins_with',
] as const;
export const singleOperatorValues = [
  ...keySingleOperatorValues,
  '<>',
  'contains',
] as const;

export type RangeOperator = TupleType<typeof rangeOperatorValues>;
export type ArrayOperator = TupleType<typeof arrayOperatorValues>;
export type SingleOperator = TupleType<typeof singleOperatorValues>;
export type KeySingleOperator = TupleType<typeof keySingleOperatorValues>;
export type AttrCheckOperator = TupleType<typeof attributeCheckValues>;
export type KeySingleComparison<U> = [KeySingleOperator, U];
export type SingleComparison<U> = [SingleOperator, U];
export type RangeComparison<U> = [RangeOperator, U, U];
export type ArrayComparison<U> = [ArrayOperator, U[]];

type CompareExpression<
  T,
  U extends keyof T,
  C extends
    | RangeComparison<T[U]>
    | SingleComparison<T[U]>
    | KeySingleComparison<T[U]>
    | ArrayComparison<T[U]>
    | AttrCheckOperator,
> = {
  key: Extract<U, string>;
  comparison: C;
};

export type RangeCompareExpression<T, U extends keyof T> = CompareExpression<
  T,
  U,
  RangeComparison<T[U]>
>;
export type SingleCompareExpression<T, U extends keyof T> = CompareExpression<
  T,
  U,
  SingleComparison<T[U]>
>;
export type KeySingleCompareExpression<
  T,
  U extends keyof T,
> = CompareExpression<T, U, KeySingleComparison<T[U]>>;
export type ArrayCompareExpression<T, U extends keyof T> = CompareExpression<
  T,
  U,
  ArrayComparison<T[U]>
>;
export type AttributeCheckExpression<T, U extends keyof T> = CompareExpression<
  T,
  U,
  AttrCheckOperator
>;

export type KeyCompareExpression<T, U extends keyof T> = CompareExpression<
  T,
  U,
  RangeComparison<T[U]> | KeySingleComparison<T[U]>
>;

export type AndCompareExpression<T> = AndGroup<SimpleExpression<T, keyof T>>;
export type OrCompareExpression<T> = OrGroup<SimpleExpression<T, keyof T>>;
export type KeyExpression<T, U extends keyof T> =
  | RangeCompareExpression<T, U>
  | KeySingleCompareExpression<T, U>;
export type AttrValueCompareExpression<T, U extends keyof T> =
  | RangeCompareExpression<T, U>
  | SingleCompareExpression<T, U>
  | ArrayCompareExpression<T, U>
  | KeyExpression<T, U>;
export type SimpleExpression<T, U extends keyof T> =
  | AttrValueCompareExpression<T, U>
  | AttributeCheckExpression<T, U>;
export type FilterExpression<T, U extends keyof T> = SimpleExpression<T, U>;

type ExpressionInfoTag = { tag: 'single' | 'range' | 'array' | 'attrCheck' };

type id = string;
type ID<T> = [id, T[keyof T]];
type SingleIdValues<T> = ID<T>;
type RangeIdValues<T> = [ID<T>, ID<T>];
type ArrayIdValues<T> = ID<T>[];
type IdValues<T> = SingleIdValues<T> | RangeIdValues<T> | ArrayIdValues<T>;

export type Condition<T> = ExpressionInfoTag & {
  key: Extract<keyof T, string>;
  operator: Operator;
  idValueKeys: IdValues<T>;
};
export type RangeConditionExpressionInfo<T> = ExpressionInfoTag & {
  key: Extract<keyof T, string>;
  tag: 'range';
  operator: RangeOperator;
  idValueKeys: RangeIdValues<T>;
};
export type SingleConditionExpressionInfo<T> = ExpressionInfoTag & {
  key: Extract<keyof T, string>;
  tag: 'single';
  operator: SingleOperator;
  idValueKeys: SingleIdValues<T>;
};
export type KeySingleConditionExpressionInfo<T> = ExpressionInfoTag & {
  key: Extract<keyof T, string>;
  tag: 'single';
  operator: KeySingleOperator;
  idValueKeys: SingleIdValues<T>;
};
export type ArrayConditionExpressionInfo<T> = ExpressionInfoTag & {
  key: Extract<keyof T, string>;
  tag: 'array';
  operator: ArrayOperator;
  idValueKeys: ArrayIdValues<T>;
};

export type AttributeCheckExpressionInfo<T> = ExpressionInfoTag & {
  key: Extract<keyof T, string>;
  tag: 'attrCheck';
  operator: AttrCheckOperator;
};

export type KeyConditionExpressionInfo<T> =
  | KeySingleConditionExpressionInfo<T>
  | RangeConditionExpressionInfo<T>;
export type SimpleConditionExpressionInfo<T> =
  | SingleConditionExpressionInfo<T>
  | RangeConditionExpressionInfo<T>
  | ArrayConditionExpressionInfo<T>
  | AttributeCheckExpressionInfo<T>;
export type FilterConditionExpressionInfo<T> = SimpleConditionExpressionInfo<T>;

export type AndGroup<T> = {
  $and: ConditionMap<T>[];
};

export type OrGroup<T> = {
  $or: ConditionMap<T>[];
};

export type NotGroup<T> = {
  $not: ConditionMap<T>;
};

export type ConditionGroup<T> = AndGroup<T> | OrGroup<T> | NotGroup<T>;

export type ConditionMap<T> = T | ConditionGroup<T>;

export type Conditions<T, U extends keyof T> = ConditionMap<
  SimpleExpression<T, U>
>;
export type ExpressionInfo<T> = ConditionMap<SimpleConditionExpressionInfo<T>>;
export type KeyConditions<T> =
  | KeyExpression<T, keyof T>
  | AndGroup<KeyExpression<T, keyof T>>;
export type KeyExpressionInfo<T> =
  | KeyConditionExpressionInfo<T>
  | AndGroup<KeyConditionExpressionInfo<T>>;
  
export type QueryOptions<T> = {
  filters?: Conditions<T, keyof T>;
  index?: IndexName;
  limit?: number;
  sort?: 'asc' | 'desc';
  projection?: Extract<keyof T, string>[];
  offsetKey?: Partial<T>;
};

export type QueryResult<T> = {
  items: T[];
  offsetKey?: Key;
};

export type AndOrGroupTag = 'and' | 'or';
export type NotTag = 'not';
export type GroupTag = AndOrGroupTag | NotTag;
