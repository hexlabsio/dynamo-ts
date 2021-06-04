
export type Operator = SingleOperator | RangeOperator | ArrayOperator | AttrCheckOperator;
export type TupleType<T extends readonly any[]> = T extends readonly [infer HEAD] ? HEAD : (T extends readonly [infer HEAD, ...infer TAIL] ? HEAD | TupleType<TAIL> : never);

export const attributeCheckValues = ['attribute_exists', 'attribute_not_exists'] as const;
export const rangeOperatorValues = ['between'] as const;
export const arrayOperatorValues = ['in'] as const;
export const singleOperatorValues = ['=', '<=', '<', '>=', '>', 'begins_with'] as const;

export type RangeOperator = TupleType<typeof rangeOperatorValues>
export type ArrayOperator = TupleType<typeof arrayOperatorValues>;
export type SingleOperator = TupleType<typeof singleOperatorValues>;
export type AttrCheckOperator = TupleType<typeof attributeCheckValues>;
export type SingleComparison<T, U extends T[keyof T]> = [SingleOperator, U];
export type RangeComparison<T, U extends T[keyof T]> = [RangeOperator, U, U];
export type ArrayComparison<T, U extends T[keyof T]> = [ArrayOperator, U[]];

export type RangeCompareExpression<T> = {
    key: Extract<keyof T, string>,
    comparison: RangeComparison<T, T[keyof T]>;
};
export type SingleCompareExpression<T> = {
    key: Extract<keyof T, string>,
    comparison: SingleComparison<T, T[keyof T]>;
};
export type ArrayCompareExpression<T> = {
    key: Extract<keyof T, string>,
    comparison: ArrayComparison<T, T[keyof T]>;
};

export type AttributeCheckExpression<T> = {
    key: Extract<keyof T, string>,
    comparison: AttrCheckOperator;
};

export type AndCompareExpression<T> = AndGroup<SimpleExpression<T>>;
export type OrCompareExpression<T> = OrGroup<SimpleExpression<T>>;
export type KeyExpression<T> = RangeCompareExpression<T> | SingleCompareExpression<T>;
export type AttrValueCompareExpression<T> = RangeCompareExpression<T> | SingleCompareExpression<T> | ArrayCompareExpression<T>;
export type SimpleExpression<T> = AttrValueCompareExpression<T> | AttributeCheckExpression<T>;
export type FilterExpression<T> = SimpleExpression<T>;


type ExpressionInfoTag = { tag: 'single' | 'range' | 'array' | 'attrCheck'; };

type id = string;
type ID<T> = [id, T[keyof T]];
type SingleIdValues<T> = ID<T>;
type RangeIdValues<T> = [ID<T>, ID<T>];
type ArrayIdValues<T> = ID<T>[];
type IdValues<T> = SingleIdValues<T> | RangeIdValues<T> | ArrayIdValues<T>;


export type Condition<T> = ExpressionInfoTag & {
    key: Extract<keyof T, string>,
    operator: Operator,
    idValueKeys: IdValues<T>;
};
export type RangeConditionExpressionInfo<T> = ExpressionInfoTag & {
    key: Extract<keyof T, string>,
    tag: 'range',
    operator: RangeOperator,
    idValueKeys: RangeIdValues<T>;
};
export type SingleConditionExpressionInfo<T> = ExpressionInfoTag & {
    key: Extract<keyof T, string>,
    tag: 'single',
    operator: SingleOperator,
    idValueKeys: SingleIdValues<T>;
};
export type ArrayConditionExpressionInfo<T> = ExpressionInfoTag & {
    key: Extract<keyof T, string>,
    tag: 'array',
    operator: ArrayOperator,
    idValueKeys: ArrayIdValues<T>;
};

export type AttributeCheckExpressionInfo<T> = ExpressionInfoTag & {
    key: Extract<keyof T, string>,
    tag: 'attrCheck',
    operator: AttrCheckOperator
};

export type KeyConditionExpressionInfo<T> = SingleConditionExpressionInfo<T> | RangeConditionExpressionInfo<T>;
export type SimpleConditionExpressionInfo<T> = SingleConditionExpressionInfo<T> | RangeConditionExpressionInfo<T> | ArrayConditionExpressionInfo<T> | AttributeCheckExpressionInfo<T>;
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

export type Conditions<T> = ConditionMap<SimpleExpression<T>>;
export type ExpressionInfo<T> = ConditionMap<SimpleConditionExpressionInfo<T>>;
export type KeyConditions<T> = KeyExpression<T> | AndGroup<KeyExpression<T>>;
export type KeyExpressionInfo<T> = KeyConditionExpressionInfo<T> | AndGroup<KeyConditionExpressionInfo<T>>;
