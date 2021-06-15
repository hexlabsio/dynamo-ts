import { DocumentClient } from 'aws-sdk/clients/dynamodb';

import QueryInput = DocumentClient.QueryInput;
import AttributeMap = DocumentClient.AttributeMap;
import { KeyConditions, QueryOptions, QueryResult } from './dynamoTypes';
import { FilterExpressions, KeyExpressions, ProjectionAttrs } from './parsers';
import { PromiseResult } from 'aws-sdk/lib/request';
import { AWSError } from 'aws-sdk/lib/error';

export class DDBClient {
  constructor(readonly documentClient: DocumentClient) {}

  async query<T>(
    tableName: string,
    key: KeyConditions<T>,
    options: QueryOptions<T> = {},
    transform: (attributeMap: AttributeMap) => T = (attributeMap) =>
      attributeMap as T,
  ): Promise<QueryResult<T>> {
    const queryInput = queryInputFrom(tableName, key, options);
    return await this.recursiveQuery(queryInput, transform);
  }

  private async recursiveQuery<T>(
    queryInput: QueryInput,
    transform: (attributeMap: AttributeMap) => T,
    results: T[] = [],
  ): Promise<QueryResult<T>> {
    const queryOutput = await this.documentClient.query(queryInput).promise();
    const queryResults = queryOutput?.Items?.map(transform) ?? [];
    const queryResultLength = queryResults.length;
    if (
      queryOutput.LastEvaluatedKey &&
      queryInput.Limit &&
      queryResultLength < queryInput.Limit
    ) {
      return await this.recursiveQuery(
        {
          ...queryInput,
          Limit: queryInput.Limit - queryResultLength,
          ExclusiveStartKey: queryOutput.LastEvaluatedKey,
        },
        transform,
        [...results, ...queryResults],
      );
    } else {
      return {
        items: [...results, ...queryResults],
        offsetKey: queryOutput.LastEvaluatedKey,
      };
    }
  }

  batchGet(
    params: DocumentClient.BatchGetItemInput,
  ): Promise<PromiseResult<DocumentClient.BatchGetItemOutput, AWSError>> {
    return this.documentClient.batchGet(params).promise();
  }

  batchWrite(
    params: DocumentClient.BatchWriteItemInput,
  ): Promise<PromiseResult<DocumentClient.BatchWriteItemOutput, AWSError>> {
    return this.documentClient.batchWrite(params).promise();
  }

  delete(
    params: DocumentClient.DeleteItemInput,
  ): Promise<PromiseResult<DocumentClient.DeleteItemOutput, AWSError>> {
    return this.documentClient.delete(params).promise();
  }
  /**
   * Returns a set of attributes for the item with the given primary key by delegating to AWS.DynamoDB.getItem().
   */
  get(
    params: DocumentClient.GetItemInput,
  ): Promise<PromiseResult<DocumentClient.GetItemOutput, AWSError>> {
    return this.documentClient.get(params).promise();
  }
  /**
   * Creates a new item, or replaces an old item with a new item by delegating to AWS.DynamoDB.putItem().
   */
  put(
    params: DocumentClient.PutItemInput,
  ): Promise<PromiseResult<DocumentClient.PutItemOutput, AWSError>> {
    return this.documentClient.put(params).promise();
  }
  /**
   * Returns one or more items and item attributes by accessing every item in a table or a secondary index.
   */
  scan(
    params: DocumentClient.ScanInput,
  ): Promise<PromiseResult<DocumentClient.ScanOutput, AWSError>> {
    return this.documentClient.scan(params).promise();
  }
  /**
   * Edits an existing item's attributes, or adds a new item to the table if it does not already exist by delegating to AWS.DynamoDB.updateItem().
   */
  update(
    params: DocumentClient.UpdateItemInput,
  ): Promise<PromiseResult<DocumentClient.UpdateItemOutput, AWSError>> {
    return this.documentClient.update(params).promise();
  }

  /**
   * Atomically retrieves multiple items from one or more tables (but not from indexes) in a single account and region.
   */
  transactGet(
    params: DocumentClient.TransactGetItemsInput,
  ): Promise<PromiseResult<DocumentClient.TransactGetItemsOutput, AWSError>> {
    return this.documentClient.transactGet(params).promise();
  }

  /**
   * Synchronous write operation that groups up to 10 action requests
   */
  transactWrite(
    params: DocumentClient.TransactWriteItemsInput,
  ): Promise<PromiseResult<DocumentClient.TransactWriteItemsOutput, AWSError>> {
    return this.documentClient.transactWrite(params).promise();
  }
}

function queryInputFrom<T>(
  tableName: string,
  key: KeyConditions<T>,
  options: QueryOptions<T>,
): QueryInput {
  const keyParser = new KeyExpressions.Parser(key);
  const queryInput: QueryInput = {
    TableName: tableName,
    KeyConditionExpression: keyParser.expression,
    ExpressionAttributeValues: keyParser.expressionAttributeValues,
    ExpressionAttributeNames: keyParser.expressionAttributeNames,
    IndexName: options.index,
    ExclusiveStartKey: options.offsetKey,
    ProjectionExpression: options?.projection?.join(', '),
  };

  if (options.filters) {
    const filterParser = new FilterExpressions.Parser(options.filters);
    Object.assign(
      queryInput.ExpressionAttributeNames,
      filterParser.expressionAttributeNames,
    );
    Object.assign(
      queryInput.ExpressionAttributeValues,
      filterParser.expressionAttributeValues,
    );
    queryInput.FilterExpression = filterParser.expression;
  }

  if (options.projection) {
    const projectionAttrs = new ProjectionAttrs.Parser(options.projection);
    Object.assign(
      queryInput.ExpressionAttributeNames,
      projectionAttrs.expressionAttributeNames,
    );
    queryInput.ProjectionExpression = projectionAttrs.projectionAttrs;
  }

  if (options.sort) {
    queryInput.ScanIndexForward = options.sort === 'asc';
  }

  if (options.limit && options.limit > 0) {
    queryInput.Limit = options.limit;
  }
  return queryInput;
}
