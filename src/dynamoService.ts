import {DynamoDB} from "aws-sdk";
import {DocumentClient} from "aws-sdk/lib/dynamodb/document_client";
import * as crypto from "crypto";
import {DDBClient} from "./client";
import {KeyConditions} from "./dynamoTypes";
import GetItemInput = DocumentClient.GetItemInput;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import {MutableQueryBuilder} from './builders/queryBuilder'

export type CollectionResult<T> = {member: T[], next?: string};
type DynamoIdentifier<H, R = never> = {hash: H, range?: R}
type DynamoKey<T, ID extends DynamoIdentifier<keyof T, keyof T>> = {[K in (ID['range'] extends string ? (ID['hash'] | ID['range']) : ID['hash'])]: T[K]}
// eslint-disable-next-line @typescript-eslint/ban-types
export class DynamoService<T, ID extends DynamoIdentifier<keyof T, keyof T>, G extends Record<string, DynamoIdentifier<keyof T, keyof T>> = {}> {
  
  public entry = undefined as unknown as T;
  public key = undefined as unknown as DynamoKey<T,ID>;
  
  constructor(protected readonly table: string, protected readonly dynamo: DynamoDB.DocumentClient) {
  }
  
  async query(keyConditions: KeyConditions<DynamoKey<T,ID>>, builder: (queryBuilder: MutableQueryBuilder<Omit<T, keyof DynamoKey<T,ID>>>) => void = () => {}): Promise<CollectionResult<T>> {
    const client = new DDBClient(this.dynamo);
    const queryBuilder = client.queryFrom<Omit<T, keyof DynamoKey<T,ID>>>(this.table, keyConditions as any);
    builder(queryBuilder);
    const result = await client.query<Omit<T, keyof DynamoKey<T,ID>>>(this.table, keyConditions as any, queryBuilder.build());
    const next = result.offsetKey ? Buffer.from(JSON.stringify(result.offsetKey!)).toString('base64') : undefined
    return {member: result.items as T[], next}
  }
  
  protected namesFrom(item: T): string[] {
    return Object.keys(item).filter(it => !!(item as any)[it]);
  }
  
  async get(key: DynamoKey<T,ID>, extras: Omit<GetItemInput, 'TableName' | 'Key'> = {}): Promise<T | undefined> {
    const result = await this.dynamo.get({TableName: this.table, Key: key, ...extras}).promise();
    return result.Item as T | undefined;
  }
  
  async getAll(hashKey: {[K in ID['hash']]: T[K]}, next?: string): Promise<CollectionResult<T>> {
    const key = Object.keys(hashKey)[0] as string;
    const hash = this.nameFor(key);
    const result = await this.dynamo.query({
      TableName: this.table,
      KeyConditionExpression: `#${hash} = :${hash}`,
      ExpressionAttributeNames: { [`#${hash}`]: key },
      ExpressionAttributeValues: { [`#${hash}`]: (hashKey as any)[key] },
      ...(next ? {ExclusiveStartKey: JSON.parse(Buffer.from(next, 'base64').toString('ascii'))} : {})
    }).promise();
    return {
      member: (result.Items ?? []) as T[],
      next: result.LastEvaluatedKey ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString('base64') : undefined
    }
  }
  
  async getAllOnIndex<K extends keyof G>(index: K, hashKey: {[TK in G[K]['hash']]: T[TK]}, next?: string): Promise<CollectionResult<T>> {
    const key = Object.keys(hashKey)[0] as string;
    const hash = this.nameFor(key);
    const result = await this.dynamo.query({
      TableName: this.table,
      IndexName: index as string,
      KeyConditionExpression: `#${hash} = :${hash}`,
      ExpressionAttributeNames: { [`#${hash}`]: key },
      ExpressionAttributeValues: { [`#${hash}`]: (hashKey as any)[key] },
      ...(next ? {ExclusiveStartKey: JSON.parse(Buffer.from(next, 'base64').toString('ascii'))} : {})
    }).promise();
    return {
      member: (result.Items ?? []) as T[],
      next: result.LastEvaluatedKey ? Buffer.from(JSON.stringify(result.LastEvaluatedKey!)).toString('base64') : undefined
    }
  }
  
  async put(item: T): Promise<T> {
    await this.dynamo.put({TableName: this.table, Item: item}).promise();
    return item;
  }
  
  async delete(key: DynamoKey<T,ID>): Promise<void> {
    await this.dynamo.delete({TableName: this.table, Key: key}).promise();
  }
  
  async update(key: DynamoKey<T,ID>, updates: Partial<Omit<T, keyof DynamoKey<T,ID>>>, increment?: keyof Omit<T, keyof DynamoKey<T,ID>>, start?: unknown, extras?: Partial<UpdateItemInput>): Promise<T | undefined> {
    const result = await this.dynamo.update({
      TableName: this.table,
      Key: key,
      ...this.updateExpression(updates, increment, start),
      ...(extras ?? {})
    }).promise();
    return result.Attributes as T | undefined;
  }
  
  private nameFor(name: string): string {
    return crypto.createHash('md5').update(name).digest('hex');
  }
  
  private updateExpression(properties: Partial<Omit<T, keyof DynamoKey<T,ID>>>, increment?: keyof Omit<T, keyof DynamoKey<T,ID>>, start?: unknown): {UpdateExpression: string, ExpressionAttributeNames: Record<string, string>, ExpressionAttributeValues: Record<string, any> } {
    const props = properties as any;
    const validKeys = Object.keys(properties).filter(it => !!props[it]);
    const removes = Object.keys(properties).filter(it => !props[it]);
    const updateExpression = `SET ${validKeys.map(key => `#${this.nameFor(key)} = ${increment === key ? `${(start !== undefined) ? `if_not_exists(${key}, :start)` : `#${this.nameFor(key)}`} + ` : ''}:${this.nameFor(key)}`).join(', ')}` +
      (removes.length > 0 ? ` REMOVE ${removes.map(key => `#${this.nameFor(key)}`).join(', ')}` : '');
    const names = [...validKeys, ...removes].reduce((names, key) => ({...names, [`#${this.nameFor(key)}`]: key}), {});
    const values = validKeys.reduce((values, key) => ({...values, [`:${this.nameFor(key)}`]: props[key]}), {});
    return {
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: (start !== undefined) ? {...values, [':start']: start} : values
    }
  }
}
