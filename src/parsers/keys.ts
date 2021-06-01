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
  singleConditionExpressionInfo, 
  mapKeyCondition
} from "./parserUtil";

import ExpressionAttributeNameMap = DocumentClient.ExpressionAttributeNameMap;
import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;

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

  keyConditionExpressionInfo<T>(compareExpression: KeyExpression<T>): KeyConditionExpressionInfo<T> {
    if (isRangeCompareExpression(compareExpression)) {
      return rangeConditionExpressionInfo(compareExpression);
    } else {
      return singleConditionExpressionInfo(compareExpression);
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
