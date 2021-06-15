import { DocumentClient } from "aws-sdk/clients/dynamodb";
import * as crypto from "crypto";
import {
    AndGroup,
    ArrayCompareExpression,
    ArrayConditionExpressionInfo,
    arrayOperatorValues,
    AttributeCheckExpression,
    AttributeCheckExpressionInfo,
    attributeCheckValues,
    AttrValueCompareExpression,
    ConditionMap,
    ExpressionInfo,
    KeySingleCompareExpression,
    KeySingleConditionExpressionInfo,
    NotGroup,
    Operator,
    OrGroup,
    RangeCompareExpression,
    RangeConditionExpressionInfo,
    rangeOperatorValues,
    SimpleConditionExpressionInfo,
    SimpleExpression,
    SingleCompareExpression,
    SingleConditionExpressionInfo
} from "../dynamoTypes";

import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;
import ExpressionAttributeNameMap = DocumentClient.ExpressionAttributeNameMap;

export function id(): string { return crypto.randomBytes(7).toString('hex'); }
export const toName = (key: string): string => `#${key}`;
export const toValue = (key: string): string => `:${key}`;


export function map<T, U>(conditionMap: ConditionMap<T>, f: (t: T) => U): ConditionMap<U> {
    const andCondition = conditionMap as AndGroup<T>;
    if (andCondition.$and) {
        return { $and: andCondition.$and.map(c => map(c, f)) };
    }
    const orCondition = conditionMap as OrGroup<T>;
    if (orCondition.$or) {
        return { $or: orCondition.$or.map(c => map(c, f)) };
    }
    const notCondition = conditionMap as NotGroup<T>;
    if (notCondition.$not) {
        return { $not: map(notCondition.$not, f) };
    }
    return f(conditionMap as T);
}

export function mapKeyCondition<T, U>(keyConditionMap: T | AndGroup<T>, f: (t: T) => U): U | AndGroup<U> {
    const andCondition = keyConditionMap as AndGroup<T>;
    if (andCondition.$and) {
        return { $and: andCondition.$and.map(c => map(c, f)) };
    }
    return f(keyConditionMap as T);
}

function reduce<T, U, R extends Record<string, U>>(conditionMap: ConditionMap<T>, f: (t: T) => R): R {
    const andCondition = conditionMap as AndGroup<T>;
    if (andCondition.$and) {
        return andCondition.$and.map(c => reduce(c, f)).reduce((acc, elem) => ({ ...acc, ...elem }), {} as R);
    }
    const orCondition = conditionMap as OrGroup<T>;
    if (orCondition.$or) {
        return orCondition.$or.map(c => reduce(c, f)).reduce((acc, elem) => ({ ...acc, ...elem }), {} as R);
    }
    const notCondition = conditionMap as NotGroup<T>;
    if (notCondition.$not) {
        return reduce(notCondition.$not, f);
    }
    return f(conditionMap as T);
}

export function expression<T>(conditionMap: ConditionMap<T>, f: (t: T) => string): string {
    const andCondition = conditionMap as AndGroup<T>;
    if (andCondition.$and) {
        return andCondition.$and.map(c => `(${expression(c, f)})`).join(" AND ");
    }
    const orCondition = conditionMap as OrGroup<T>;
    if (orCondition.$or) {
        return orCondition.$or.map(c => `(${expression(c, f)})`).join(" OR ");
    }
    const notCondition = conditionMap as NotGroup<T>;
    if (notCondition.$not) {
        return `NOT (${expression(notCondition.$not, f)})`;
    }
    const condition = conditionMap as T;
    return f(condition);
}
export function expressionAttributeNamesFrom<T>(expressionInfo: ExpressionInfo<T>): ExpressionAttributeNameMap {
    return reduce(expressionInfo, expr => ({ [toName(expr.key)]: expr.key, }));
}

export function expressionAttributeValuesFrom<T>(expressionInfo: ExpressionInfo<T>): ExpressionAttributeValueMap {
    return reduce(expressionInfo, simpleExpressionAttributeValueMap);
}

export function isAttrCheckExpression<T, U extends keyof T>(simpleExpression: SimpleExpression<T, U>): simpleExpression is AttributeCheckExpression<T, U> {
    return typeof simpleExpression.comparison === 'string' && (attributeCheckValues as readonly string[]).includes(simpleExpression.comparison);
}

export function isRangeCompareExpression<T, U extends keyof T>(attrValueCompareExpression: AttrValueCompareExpression<T, U>): attrValueCompareExpression is RangeCompareExpression<T, U> {
    return (rangeOperatorValues as readonly Operator[]).includes(attrValueCompareExpression.comparison[0]);
}

export function isArrayCompareExpression<T, U extends keyof T>(attrValueCompareExpression: AttrValueCompareExpression<T, U>): attrValueCompareExpression is ArrayCompareExpression<T, U> {
    return (arrayOperatorValues  as readonly Operator[]).includes(attrValueCompareExpression.comparison[0]);
}

export function attributeCheckExpressionInfo<T, U extends keyof T>(attributeCheckExpression: AttributeCheckExpression<T, U>): AttributeCheckExpressionInfo<T> {
    return {
        tag: 'attrCheck',
        key: attributeCheckExpression.key,
        operator: attributeCheckExpression.comparison,
    };
}

export function rangeConditionExpressionInfo<T, U extends keyof T>(rangeCompareExpression: RangeCompareExpression<T, U>): RangeConditionExpressionInfo<T> {
    return {
        tag: 'range',
        key: rangeCompareExpression.key,
        operator: rangeCompareExpression.comparison[0],
        idValueKeys: [[id(), rangeCompareExpression.comparison[1]], [id(), rangeCompareExpression.comparison[2]]]
    };
}

export function singleConditionExpressionInfo<T, U extends keyof T>(singleCompareExpression: SingleCompareExpression<T, U> | KeySingleCompareExpression<T, U>): SingleConditionExpressionInfo<T> {
    return {
        tag: 'single',
        key: singleCompareExpression.key,
        operator: singleCompareExpression.comparison[0],
        idValueKeys: [id(), singleCompareExpression.comparison[1]]
    };
}
export function keySingleConditionExpressionInfo<T, U extends keyof T>(singleCompareExpression: KeySingleCompareExpression<T, U>): KeySingleConditionExpressionInfo<T> {
    return {
        tag: 'single',
        key: singleCompareExpression.key,
        operator: singleCompareExpression.comparison[0],
        idValueKeys: [id(), singleCompareExpression.comparison[1]]
    };
}

export function arrayConditionExpressionInfo<T, U extends keyof T>(rangeCompareExpression: ArrayCompareExpression<T, U>): ArrayConditionExpressionInfo<T> {
    return {
        tag: 'array',
        key: rangeCompareExpression.key,
        operator: rangeCompareExpression.comparison[0],
        idValueKeys: rangeCompareExpression.comparison[1].map(value => [id(), value])
    };
}

export function simpleConditionExpressionInfo<T, U extends keyof T>(compareExpression: SimpleExpression<T, U>): SimpleConditionExpressionInfo<T> {
    if (isAttrCheckExpression(compareExpression)) {
        return attributeCheckExpressionInfo(compareExpression);
    } else if (isRangeCompareExpression(compareExpression)) {
        return rangeConditionExpressionInfo(compareExpression);
    } else if (isArrayCompareExpression(compareExpression)) {
        return arrayConditionExpressionInfo(compareExpression);
    } else {
        return singleConditionExpressionInfo(compareExpression);
    }
}

export function rangeConditionExpression<T>(rangeConditionExpressionInfo: RangeConditionExpressionInfo<T>): string {
    const [[id1], [id2]] = rangeConditionExpressionInfo.idValueKeys;
    //"#${k} BETWEEN :6a6f5 AND :${k}"
    return `${toName(rangeConditionExpressionInfo.key)} BETWEEN ${toValue(id1)} AND ${toValue(id2)}`;
}

export function singleConditionExpression<T>(singleConditionExpressionInfo: SingleConditionExpressionInfo<T>): string {
    if (['begins_with', 'contains'].includes(singleConditionExpressionInfo.operator)) {
        const [id] = singleConditionExpressionInfo.idValueKeys;
        return `${singleConditionExpressionInfo.operator}(${toName(singleConditionExpressionInfo.key)}, ${toValue(id)})`;
    } 
    else {
        const [id] = singleConditionExpressionInfo.idValueKeys;
        return `${toName(singleConditionExpressionInfo.key)} ${singleConditionExpressionInfo.operator} ${toValue(id)}`;
    }
}

export function arrayConditionExpression<T>(arrayConditionExpressionInfo: ArrayConditionExpressionInfo<T>): string {
    if (arrayConditionExpressionInfo.operator === 'in') {
        // `begins_with(#${k}, :${k})`
        const ids = arrayConditionExpressionInfo.idValueKeys.map(([id]) => toValue(id)).join(",");
        return `${toName(arrayConditionExpressionInfo.key)} IN (${ids})`;
    } else {
        return "";
    }
}

export function attrCheckExpression<T>(attributeCheckExpressionInfo: AttributeCheckExpressionInfo<T>): string {
    return `${attributeCheckExpressionInfo.operator}(${toName(attributeCheckExpressionInfo.key)})`;
}

export function simpleConditionExpression<T>(simpleConditionExpressionInfo: SimpleConditionExpressionInfo<T>): string {
    switch (simpleConditionExpressionInfo.tag) {
        case 'range':
            return rangeConditionExpression(simpleConditionExpressionInfo);
        case 'single':
            return singleConditionExpression(simpleConditionExpressionInfo);
        case 'array':
            return arrayConditionExpression(simpleConditionExpressionInfo);
        case 'attrCheck':
            return attrCheckExpression(simpleConditionExpressionInfo);    
    }
}

export function rangeExpressionAttributeValueMap<T>(rangeConditionExpressionInfo: RangeConditionExpressionInfo<T>): ExpressionAttributeValueMap {
    const [[id1, value1], [id2, value2]] = rangeConditionExpressionInfo.idValueKeys;
    return {
        [toValue(id1)]: value1,
        [toValue(id2)]: value2,
    };
}

export function singleExpressionAttributeValueMap<T>(singleConditionExpressionInfo: SingleConditionExpressionInfo<T>): ExpressionAttributeValueMap {
    const [id, value] = singleConditionExpressionInfo.idValueKeys;
    return {
        [toValue(id)]: value,
    };
}

export function arrayExpressionAttributeValueMap<T>(arrayConditionExpressionInfo: ArrayConditionExpressionInfo<T>): ExpressionAttributeValueMap {
    let values: ExpressionAttributeValueMap = {};
    arrayConditionExpressionInfo.idValueKeys.forEach(([id, value]) => values[toValue(id)] = value);
    return values;
}

export function simpleExpressionAttributeValueMap<T>(simpleConditionExpressionInfo: SimpleConditionExpressionInfo<T>): ExpressionAttributeValueMap {
    switch (simpleConditionExpressionInfo.tag) {
        case 'range':
            return rangeExpressionAttributeValueMap(simpleConditionExpressionInfo);
        case 'single':
            return singleExpressionAttributeValueMap(simpleConditionExpressionInfo);
        case 'array':
            return arrayExpressionAttributeValueMap(simpleConditionExpressionInfo);
        case 'attrCheck':
            return {};
    }
}
