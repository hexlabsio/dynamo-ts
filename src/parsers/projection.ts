import { DocumentClient } from "aws-sdk/clients/dynamodb";

import { toName } from "./parserUtil";

import ExpressionAttributeNameMap = DocumentClient.ExpressionAttributeNameMap;

export class Parser<T> {

  private expressionAttributeNameMap: ExpressionAttributeNameMap;

  constructor(projectionAttrs: Extract<keyof T, string>[]) {     
    this.expressionAttributeNameMap = projectionAttrs.reduce((acc, elem) => ({...acc, [toName(elem)]: elem }), {})
  }

  get expressionAttributeNames(): ExpressionAttributeNameMap {
    return this.expressionAttributeNameMap;
  }

  get projectionAttrs(): string {
    return Object.keys(this.expressionAttributeNameMap).join(", ")
  }

}
