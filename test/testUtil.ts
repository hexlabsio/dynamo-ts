
import { DocumentClient } from "aws-sdk/clients/dynamodb";

import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;
import AttributeValue = DocumentClient.AttributeValue;

export function expectAttributeValueKV<T>(attributeValues: ExpressionAttributeValueMap, lookupValue: T): [string, AttributeValue] {
    const kv = find(attributeValues, (k, v) => v === lookupValue);
    expect(kv).toBeDefined();
    return kv!;
}


function find<T>(obj: { [key: string]: T; }, pred: ((k: string, v: T) => boolean)): [string, T] | undefined {
    return fold(obj, undefined, (acc: [string, T] | undefined, k: string, v: T) =>
        (acc !== undefined && pred(k, v)) ? [k, v] : acc);
}

export function fold<T, R>(obj: { [key: string]: T; }, init: R, f: (acc: R, k: string, v: T) => R): R {
    let outObj = { ...init };
    Object.keys(obj).forEach((key) =>
        outObj = f(outObj, key, obj[key])
    );
    return outObj;
}