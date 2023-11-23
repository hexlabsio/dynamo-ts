import { UpdateCommandInput, UpdateCommandOutput } from '@aws-sdk/lib-dynamodb';
import { filterParts } from './comparison';
import { AttributeBuilder } from './attribute-builder';
import { TableDefinition } from './table-builder/table-definition';
import { CamelCaseKeys } from './types/camel-case';
import { DynamoConfig } from './types/dynamo-config';
import { DynamoFilter } from './types/filter';
import { JsonPath, ValueAtJsonPath } from './types/json-path';

export type Increment<T, K extends keyof T> = {
  key: K;
  start?: T[K];
};

export type UpdateItemOptions<
  TableConfig extends TableDefinition,
  KEY extends JsonPath<TableConfig['type']>,
  RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null = null,
> = Partial<
  CamelCaseKeys<
    Pick<
      UpdateCommandInput,
      'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'
    >
  >
> & {
  key: TableConfig['keys'];
  updates: { [K in JsonPath<TableConfig['withoutKeys']>]?: ValueAtJsonPath<K, TableConfig['withoutKeys']> };
  condition?: DynamoFilter<TableConfig['type']>;
  increments?: Array<
    Increment<
      TableConfig['type'],
      KEY
    >
  >;
  return?: RETURN_ITEMS;
};

export type UpdateReturnType<
  TableType,
  RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null,
> = RETURN_ITEMS extends null
  ? undefined
  : RETURN_ITEMS extends 'NONE'
  ? undefined
  : RETURN_ITEMS extends 'UPDATED_OLD'
  ? Partial<TableType>
  : RETURN_ITEMS extends 'UPDATED_NEW'
  ? Partial<TableType>
  : TableType;

export type UpdateResult<
  TableType,
  RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null,
> = {
  item: UpdateReturnType<TableType, RETURN_ITEMS>;
  consumedCapacity?: UpdateCommandOutput['ConsumedCapacity'];
  itemCollectionMetrics?: UpdateCommandOutput['ItemCollectionMetrics'];
};

export interface UpdateExecutor<
  TableType,
  RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null = null,
> {
  input: UpdateCommandInput;
  execute(): Promise<UpdateResult<TableType, RETURN_ITEMS>>;
}

export class DynamoUpdater<TableConfig extends TableDefinition> {
  constructor(
    private readonly clientConfig: DynamoConfig,
  ) {}

  private updateExpression(
    attributeBuilder: AttributeBuilder,
    properties: any,
    increment?: Array<Increment<any, any>>,
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
    KEY extends JsonPath<TableConfig['type']>,
    RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null = null,
  >(
    options: UpdateItemOptions<TableConfig['type'], KEY, RETURN_ITEMS>,
  ): UpdateExecutor<TableConfig['type'], RETURN_ITEMS> {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition &&
      filterParts(attributeBuilder, options.condition);
    const {
      key,
      updates,
      increments,
      return: returnValues,
      returnItemCollectionMetrics,
      returnConsumedCapacity,
    } = options;
    const updateInput: UpdateCommandInput = {
      TableName: this.clientConfig.tableName,
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
    const config = this.clientConfig;
    return {
      input: updateInput,
      async execute(): Promise<UpdateResult<TableConfig['type'], RETURN_ITEMS>> {
        const result = await config.client.update(updateInput);
        return {
          item: result.Attributes as any,
          consumedCapacity: result.ConsumedCapacity,
          itemCollectionMetrics: result.ItemCollectionMetrics,
        };
      },
    };
  }

  update<
    KEY extends JsonPath<TableConfig['type']>,
    RETURN_ITEMS extends UpdateCommandInput['ReturnValues'] | null = null,
  >(
    options: UpdateItemOptions<TableConfig, KEY, RETURN_ITEMS>,
  ): Promise<UpdateResult<TableConfig['type'], RETURN_ITEMS>> {
    const executor = this.updateExecutor(options);
    if (this.clientConfig.logStatements) {
      console.log(
        `UpdateItemInput: ${JSON.stringify(executor.input, null, 2)}`,
      );
    }
    return executor.execute();
  }
}
