import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import {
  DynamoEntry,
  DynamoKeysFrom,
  DynamoMapDefinition,
  DynamoNonKeysFrom,
  DynamoRangeKey,
} from './type-mapping';
import { DynamoClientConfig } from './dynamo-client-config';
import { AttributeBuilder } from './attribute-builder';
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;
import ReturnValue = DocumentClient.ReturnValue;

export type Increment<T, K extends keyof T> = {
  key: K;
  start?: T[K];
};

export type UpdateItemOptions<
  DEFINITION extends DynamoMapDefinition,
  HASH extends keyof DynamoEntry<DEFINITION>,
  RANGE extends DynamoRangeKey<DEFINITION, HASH>,
  KEY extends keyof DynamoEntry<DEFINITION>,
  RETURN_ITEMS extends ReturnValue | null = null,
> = Partial<
  Pick<
    UpdateItemInput,
    'ReturnValues' | 'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'
  >
> & {
  key: DynamoKeysFrom<DEFINITION, HASH, RANGE>;
  updates: Partial<DynamoNonKeysFrom<DEFINITION, HASH, RANGE>>;
  increments?: Array<Increment<DynamoEntry<DEFINITION>, KEY>>;
  return?: RETURN_ITEMS;
};

export type UpdateReturnType<
  DEFINITION extends DynamoMapDefinition,
  RETURN_ITEMS extends ReturnValue | null,
> = RETURN_ITEMS extends null
  ? undefined
  : RETURN_ITEMS extends 'NONE'
  ? undefined
  : RETURN_ITEMS extends 'UPDATED_OLD'
  ? Partial<DynamoClientConfig<DEFINITION>['tableType']>
  : RETURN_ITEMS extends 'UPDATED_NEW'
  ? Partial<DynamoClientConfig<DEFINITION>['tableType']>
  : DynamoClientConfig<DEFINITION>['tableType'];

export class DynamoUpdater {
  private static updateExpression<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends DynamoRangeKey<DEFINITION, HASH>,
    KEY extends keyof DynamoEntry<DEFINITION>,
  >(
    attributeBuilder: AttributeBuilder,
    properties: Partial<DynamoNonKeysFrom<DEFINITION, HASH, RANGE>>,
    increment?: Array<Increment<DynamoEntry<DEFINITION>, KEY>>,
  ): string {
    const props = properties as any;
    const propKeys = Object.keys(properties);
    const validKeys = propKeys.filter((it) => props[it] !== undefined);
    const removes = propKeys.filter((it) => props[it] === undefined);

    attributeBuilder.addNames(...propKeys);

    function update(key: string) {
      const inc = (increment ?? []).find((it) => it.key === key);
      if (inc)
        return (
          `${attributeBuilder.nameFor(key)} = ` +
          (inc.start !== undefined
            ? `if_not_exists(${attributeBuilder.nameFor(
                key,
              )}, ${attributeBuilder.addValue(inc.start)})`
            : `${attributeBuilder.nameFor(key)}`) +
          ` + ${attributeBuilder.addValue(key)}`
        );
      return `${attributeBuilder.nameFor(key)} = ${attributeBuilder.addValue(
        props[key],
      )}`;
    }

    const setExpression =
      validKeys.length > 0
        ? `SET ${validKeys
            .map((key) => update(key))
            .filter((it) => !!it)
            .join(', ')}`
        : undefined;
    const removeExpression =
      removes.length > 0
        ? `REMOVE ${removes
            .map((key) => `${attributeBuilder.nameFor(key)} `)
            .join(', ')}`
        : undefined;
    return [setExpression, removeExpression].filter((it) => !!it).join(' ');
  }

  static async update<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoEntry<DEFINITION>,
    RANGE extends DynamoRangeKey<DEFINITION, HASH>,
    KEY extends keyof DynamoEntry<DEFINITION>,
    RETURN_ITEMS extends ReturnValue | null = null,
  >(
    config: DynamoClientConfig<DEFINITION>,
    options: UpdateItemOptions<DEFINITION, HASH, RANGE, KEY, RETURN_ITEMS>,
  ): Promise<{
    item: UpdateReturnType<DEFINITION, RETURN_ITEMS>;
    consumedCapacity?: ConsumedCapacity;
    itemCollectionMetrics?: ItemCollectionMetrics;
  }> {
    const attributeBuilder = AttributeBuilder.create();
    const {
      key,
      updates,
      increments,
      return: returnValues,
      ...extras
    } = options;
    const updateInput: UpdateItemInput = {
      TableName: config.tableName,
      Key: key,
      UpdateExpression: this.updateExpression(
        attributeBuilder,
        updates,
        increments,
      ),
      ReturnValues: returnValues ?? undefined,
      ...extras,
      ...attributeBuilder.asInput(),
    };
    if (config.logStatements) {
      console.log(`UpdateItemInput: ${JSON.stringify(updateInput, null, 2)}`);
    }
    const result = await config.client.update(updateInput).promise();
    return {
      item: result.Attributes as any,
      consumedCapacity: result.ConsumedCapacity,
      itemCollectionMetrics: result.ItemCollectionMetrics,
    };
  }
}
