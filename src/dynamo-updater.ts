import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import {
  DynamoEntry,
  DynamoKeysFrom,
  DynamoMapDefinition
} from "./type-mapping";
import {DynamoClientConfig, DynamoKeys} from "./dynamo-client-config";
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import DeleteItemInput = DocumentClient.DeleteItemInput;
import {ComparisonBuilder} from "./comparison";
import {CompareWrapperOperator} from "./operation";
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;
import {AttributeBuilder, AttributeNamer} from "./naming";

export type DeleteItemOptions<DEFINITION extends DynamoMapDefinition, RETURN_OLD extends boolean = false> = Pick<DeleteItemInput, 'ExpressionAttributeNames' | 'ExpressionAttributeValues' | 'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'> &
    {
      condition?: (compare: () => ComparisonBuilder<DEFINITION>) => CompareWrapperOperator<DEFINITION>,
      returnOldValues?: RETURN_OLD
    }

export type Increment<DEFINITION extends DynamoMapDefinition, K extends keyof DynamoEntry<DEFINITION>> = {
  key: K,
  start?: DynamoEntry<DEFINITION>[K]
}

export class DynamoUpdater {

  updateExpression<
      DEFINITION extends DynamoMapDefinition,
      HASH extends keyof DynamoEntry<DEFINITION>,
      RANGE extends keyof DynamoEntry<DEFINITION> | null,
      KEY extends keyof DynamoEntry<DEFINITION>
  > (
      properties: Partial<Omit<DynamoEntry<DEFINITION>, KEY>>,
      increment?: Array<{
        key: KEY,
        start?: DynamoEntry<DEFINITION>[KEY]
      }>
  ): {
    expression: string;
    attributeBuilder: AttributeBuilder;
  } {
    const props = properties as any;
    const validKeys = Object.keys(properties).filter((it) => props[it] !== undefined);
    const removes = Object.keys(properties).filter((it) => props[it] === undefined);

    function update(key: string, name: string) {
      const inc = (increment ?? []).find(it => it.key === key);
      if(inc) return `#${name} = ` + (inc.start !== undefined ? `if_not_exists(#${name}, :${name}start)`: `#${name}`) + ` + :${name}`
      return `#${name} = :${name}`;
    }

    const setExpression = validKeys.length > 0
        ? `SET ${validKeys.map((key) => update(key, AttributeNamer.nameFor(key))).filter(it => !!it).join(', ')}` : undefined
    const removeExpression = removes.length > 0
        ? `REMOVE ${removes.map((key) => `#${AttributeNamer.nameFor(key)}`).join(', ')}` : undefined
    const updateExpression = [setExpression, removeExpression].filter(it => !!it).join(' ')

    const names = [...validKeys, ...removes].reduce(
        (names, key) => ({ ...names, [`#${AttributeNamer.nameFor(key)}`]: key }),
        {},
    );
    const values = validKeys.reduce(
        (values, key) => ({ ...values, [`:${AttributeNamer.nameFor(key)}`]: props[key] }),
        {},
    );
    const starts = (increment ?? []).filter(it => it.start !== undefined).reduce((acc, increment) =>
        ({...acc, [`:${AttributeNamer.nameFor(increment.key as string)}start`]: increment.start}), {})
    return {
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: (increment?.length ?? 0) + (validKeys.length) > 0
          ? { ...values, ...starts }
          : undefined
    };
  }

}