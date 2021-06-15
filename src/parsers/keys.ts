import { DocumentClient } from "aws-sdk/clients/dynamodb";
import {
  KeyConditions,
  KeyExpressionInfo,
  KeyExpression,
  KeyConditionExpressionInfo
} from "../dynamoTypes";
import { 
  simpleConditionExpression,
  expression,
  expressionAttributeValuesFrom,
  expressionAttributeNamesFrom,
  isRangeCompareExpression,
  rangeConditionExpressionInfo,
  mapKeyCondition,
  keySingleConditionExpressionInfo
} from "./parserUtil";

import ExpressionAttributeNameMap = DocumentClient.ExpressionAttributeNameMap;
import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;

/**
 * From https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions
 * You must specify the partition key name and value as an equality condition.
 * You can optionally provide a second condition for the sort key (if present). The sort key condition must use one of the following comparison operators:
 * 
 *     a = b — true if the attribute a is equal to the value b
 *     a < b — true if a is less than b
 *     a <= b — true if a is less than or equal to b
 *     a > b — true if a is greater than b
 *     a >= b — true if a is greater than or equal to b
 *     a BETWEEN b AND c — true if a is greater than or equal to b, and less than or equal to c.
 * 
 * The following function is also supported:
 *     begins_with (a, substr)— true if the value of attribute a begins with a particular substring.
 * 
 */
export class Parser<T> {
  private keyExpressionInfo: KeyExpressionInfo<T>;

  constructor(keyConditions: KeyConditions<T>) {
    this.keyExpressionInfo = mapKeyCondition(keyConditions, this.keyConditionExpressionInfo);
  }

  get expressionAttributeNames(): ExpressionAttributeNameMap {
    return expressionAttributeNamesFrom(this.keyExpressionInfo);
  }

  get expressionAttributeValues(): ExpressionAttributeValueMap {
    return expressionAttributeValuesFrom(this.keyExpressionInfo);
  }

  get expression(): string {
    return expression(this.keyExpressionInfo, simpleConditionExpression);
  }

  keyConditionExpressionInfo<T, U extends keyof T>(compareExpression: KeyExpression<T, U>): KeyConditionExpressionInfo<T> {
    if (isRangeCompareExpression(compareExpression)) {
      return rangeConditionExpressionInfo(compareExpression);
    } else {
      return keySingleConditionExpressionInfo(compareExpression);
    }
  }
  toString(): string {
    return JSON.stringify({
      ExpressionAttributeNames: this.expressionAttributeNames,
      ExpressionAttributeValues: this.expressionAttributeValues,
      Expression: this.expression
    }, null, 2);
  }
}
