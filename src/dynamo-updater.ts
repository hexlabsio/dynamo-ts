import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import { filterPartsWithKey } from "./comparison";
import { DynamoFilter2 } from './filter';
import { DynamoNestedKV } from './type-mapping';
import { AttributeBuilder } from './attribute-builder';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoInfo,
  PickKeys,
  TypeFromDefinition,
} from './types';
import ConsumedCapacity = DocumentClient.ConsumedCapacity;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import ItemCollectionMetrics = DocumentClient.ItemCollectionMetrics;
import ReturnValue = DocumentClient.ReturnValue;

export type Increment<T, K extends keyof T> = {
  key: K;
  start?: T[K];
};

export type UpdateItemOptions<
  T extends DynamoInfo,
  KEY extends keyof DynamoNestedKV<TypeFromDefinition<T['definition']>>,
  RETURN_ITEMS extends ReturnValue | null = null,
> = Partial<
  CamelCaseKeys<
    Pick<
      UpdateItemInput,
      'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'
    >
  >
> & {
  key: PickKeys<T>;
  updates: DynamoNestedKV<
    Omit<TypeFromDefinition<T['definition']>, keyof PickKeys<T>>
  >;
  condition?: DynamoFilter2<T>;
  increments?: Array<
    Increment<
      DynamoNestedKV<
        Omit<TypeFromDefinition<T['definition']>, keyof PickKeys<T>>
      >,
      KEY
    >
  >;
  return?: RETURN_ITEMS;
};

export type UpdateReturnType<
  T extends DynamoInfo,
  RETURN_ITEMS extends ReturnValue | null,
> = RETURN_ITEMS extends null
  ? undefined
  : RETURN_ITEMS extends 'NONE'
  ? undefined
  : RETURN_ITEMS extends 'UPDATED_OLD'
  ? Partial<TypeFromDefinition<T['definition']>>
  : RETURN_ITEMS extends 'UPDATED_NEW'
  ? Partial<TypeFromDefinition<T['definition']>>
  : TypeFromDefinition<T['definition']>;

export type UpdateResult<
  T extends DynamoInfo,
  RETURN_ITEMS extends ReturnValue | null,
> = {
  item: UpdateReturnType<T, RETURN_ITEMS>;
  consumedCapacity?: ConsumedCapacity;
  itemCollectionMetrics?: ItemCollectionMetrics;
};

export interface UpdateExecutor<
  T extends DynamoInfo,
  RETURN_ITEMS extends ReturnValue | null = null,
> {
  input: UpdateItemInput;
  execute(): Promise<UpdateResult<T, RETURN_ITEMS>>;
}

export class DynamoUpdater<T extends DynamoInfo> {
  constructor(
    private readonly info: T,
    private readonly config: DynamoConfig,
  ) {}

  private updateExpression<
    KEY extends keyof DynamoNestedKV<TypeFromDefinition<T['definition']>>,
  >(
    attributeBuilder: AttributeBuilder,
    properties: DynamoNestedKV<
      Omit<TypeFromDefinition<T['definition']>, keyof PickKeys<T>>
    >,
    increment?: Array<
      Increment<
        DynamoNestedKV<
          Omit<TypeFromDefinition<T['definition']>, keyof PickKeys<T>>
        >,
        KEY
      >
    >,
  ): string {
    const props = properties as any;
    const propKeys = Object.keys(properties);
    const validKeys = propKeys.filter((it) => props[it] !== undefined);
    const removes = propKeys.filter((it) => props[it] === undefined);

    function setterFor(key: string) {
      const inc = (increment ?? []).find((it) => it.key === key);
      if (inc)
        return (
          `${attributeBuilder.buildPath(key)} = ` +
          (inc.start !== undefined
            ? `if_not_exists(${attributeBuilder.buildPath(
                key,
              )}, ${attributeBuilder.addValue(inc.start)})`
            : `${attributeBuilder.buildPath(key)}`) +
          ` + ${attributeBuilder.addValue(props[key])}`
        );
      return `${attributeBuilder.buildPath(key)} = ${attributeBuilder.addValue(
        props[key],
      )}`;
    }

    const setExpression =
      validKeys.length > 0
        ? `SET ${validKeys
            .map((key) => setterFor(key))
            .filter((it) => !!it)
            .join(', ')}`
        : undefined;
    const removeExpression =
      removes.length > 0
        ? `REMOVE ${removes
            .map((key) => `${attributeBuilder.buildPath(key)} `)
            .join(', ')}`
        : undefined;
    return [setExpression, removeExpression].filter((it) => !!it).join(' ');
  }

  updateExecutor<
    KEY extends keyof DynamoNestedKV<TypeFromDefinition<T['definition']>>,
    RETURN_ITEMS extends ReturnValue | null = null,
  >(
    options: UpdateItemOptions<T, KEY, RETURN_ITEMS>,
  ): UpdateExecutor<T, RETURN_ITEMS> {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition &&
      filterPartsWithKey(this.info, attributeBuilder, options.condition);
    const {
      key,
      updates,
      increments,
      return: returnValues,
      returnItemCollectionMetrics,
      returnConsumedCapacity,
    } = options;
    const updateInput: UpdateItemInput = {
      TableName: this.config.tableName,
      ConditionExpression: condition,
      Key: key,
      UpdateExpression: this.updateExpression(
        attributeBuilder,
        updates,
        increments,
      ),
      ReturnValues: returnValues ?? undefined,
      ReturnItemCollectionMetrics: returnItemCollectionMetrics,
      ReturnConsumedCapacity: returnConsumedCapacity,
      ...attributeBuilder.asInput(),
    };
    const config = this.config;
    return {
      input: updateInput,
      async execute(): Promise<UpdateResult<T, RETURN_ITEMS>> {
        const result = await config.client.update(updateInput).promise();
        return {
          item: result.Attributes as any,
          consumedCapacity: result.ConsumedCapacity,
          itemCollectionMetrics: result.ItemCollectionMetrics,
        };
      },
    };
  }

  update<
    KEY extends keyof DynamoNestedKV<TypeFromDefinition<T['definition']>>,
    RETURN_ITEMS extends ReturnValue | null = null,
  >(
    options: UpdateItemOptions<T, KEY, RETURN_ITEMS>,
  ): Promise<UpdateResult<T, RETURN_ITEMS>> {
    const executor = this.updateExecutor(options);
    if (this.config.logStatements) {
      console.log(
        `UpdateItemInput: ${JSON.stringify(executor.input, null, 2)}`,
      );
    }
    return executor.execute();
  }
}
