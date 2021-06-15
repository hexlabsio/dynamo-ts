import { DocumentClient } from "aws-sdk/clients/dynamodb";
import {
  Conditions,
  ExpressionInfo
} from "../dynamoTypes";
import { 
  map, 
  expressionAttributeNamesFrom, 
  expressionAttributeValuesFrom, 
  expression,
  simpleConditionExpressionInfo, 
  simpleConditionExpression } from "./parserUtil";

import ExpressionAttributeNameMap = DocumentClient.ExpressionAttributeNameMap;
import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;

export class Parser<T> {
  private expressionInfo: ExpressionInfo<T>;

  constructor(condition: Conditions<T, keyof T>) {
    this.expressionInfo = map(condition, simpleConditionExpressionInfo)
  }

  get expressionAttributeNames(): ExpressionAttributeNameMap {
    return expressionAttributeNamesFrom(this.expressionInfo)
  }

  get expressionAttributeValues(): ExpressionAttributeValueMap {
    return expressionAttributeValuesFrom(this.expressionInfo)
  }

  get expression(): string {
    return expression(this.expressionInfo, simpleConditionExpression)
  }

  toString(): string {
    return JSON.stringify({
      ExpressionAttributeNames: this.expressionAttributeNames,
      ExpressionAttributeValues: this.expressionAttributeValues,
      Expression: this.expression
    }, null, 2);
  }
}
