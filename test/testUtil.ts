import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { AWSError, Request } from 'aws-sdk';

import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;
import AttributeValue = DocumentClient.AttributeValue;

import QueryInput = DocumentClient.QueryInput;
import QueryOutput = DocumentClient.QueryOutput;

export function expectAttributeValueKV<T>(
  attributeValues: ExpressionAttributeValueMap,
  lookupValue: T,
): [string, AttributeValue] {
  const kv = find(attributeValues, (k, v) => v === lookupValue);
  expect(kv).toBeDefined();
  return kv!;
}

function find<T>(
  obj: { [key: string]: T },
  pred: (k: string, v: T) => boolean,
): [string, T] | undefined {
  return fold(obj, undefined, (acc: [string, T] | undefined, k: string, v: T) =>
    acc !== undefined && pred(k, v) ? [k, v] : acc,
  );
}

export function fold<T, R>(
  obj: { [key: string]: T },
  init: R,
  f: (acc: R, k: string, v: T) => R,
): R {
  let outObj = { ...init };
  Object.keys(obj).forEach((key) => (outObj = f(outObj, key, obj[key])));
  return outObj;
}

export function partialAs<T>(pt: Partial<T>): T {
  return pt as T;
}

//mock utils
export function ddbMock(queryFn: jest.Mock): DocumentClient {
  return partialAs<DocumentClient>({
    query: queryFn,
    get: jest.fn(),
    put: jest.fn(),
    update: jest.fn(),
    batchWrite: jest.fn(),
  });
}

export const success: <T>(response: T) => Request<T, AWSError> = <T>(
  response: T,
) => ({ promise: async () => response } as Request<T, AWSError>);

export const mockDDBquery: (
  query: QueryInput,
) => Request<QueryOutput, AWSError> = () => {
  return success({
    Count: 0,
    Items: [],
    ScannedCount: 0,
  });
};
