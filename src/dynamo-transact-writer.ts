import {
  ConditionCheck,
  Delete,
  Put,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  TransactWriteItemsInput,
  Update,
} from '@aws-sdk/client-dynamodb/dist-types/models';
import { TransactWriteItemsOutput } from '@aws-sdk/client-dynamodb/dist-types/models/models_0';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { AttributeBuilder } from './attribute-builder';
import { filterParts } from './comparison';
import { DynamoUpdater, Increment } from './dynamo-updater';
import { TableDefinition } from './table-builder/table-definition';
import {
  CamelCaseKeys,
  DynamoConfig,
  DynamoFilter,
  JsonPath,
  ValueAtJsonPath,
} from './types';

export type TransactPutOptions<TableType> = CamelCaseKeys<
  Pick<Put, 'ReturnValuesOnConditionCheckFailure'>
> & {
  item: TableType;
  condition?: DynamoFilter<TableType>;
};

export type TransactUpdateOptions<
  TableConfig extends TableDefinition,
  KEY extends JsonPath<TableConfig['type']>,
> = CamelCaseKeys<Pick<Update, 'ReturnValuesOnConditionCheckFailure'>> & {
  key: TableConfig['keys'];
  updates: {
    [K in JsonPath<TableConfig['withoutKeys']>]?: ValueAtJsonPath<
      K,
      TableConfig['withoutKeys']
    >;
  };
  condition?: DynamoFilter<TableConfig['type']>;
  increments?: Array<Increment<TableConfig['type'], KEY>>;
};

export type TransactDeleteOptions<TableConfig extends TableDefinition> =
  CamelCaseKeys<Pick<Delete, 'ReturnValuesOnConditionCheckFailure'>> & {
    key: TableConfig['keys'];
    condition?: DynamoFilter<TableConfig['type']>;
  };

export type TransactConditionOptions<TableConfig extends TableDefinition> =
  CamelCaseKeys<Pick<ConditionCheck, 'ReturnValuesOnConditionCheckFailure'>> & {
    key: TableConfig['keys'];
    condition: DynamoFilter<TableConfig['type']>;
  };

export type TransactWriteOptions = {
  returnConsumedCapacity?: ReturnConsumedCapacity;
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics;
  clientRequestToken?: string;
};
export type TransactWriteReturn = CamelCaseKeys<TransactWriteItemsOutput>;

export interface TransactWriteExecutor {
  input: TransactWriteItemsInput;
  execute(options: TransactWriteOptions): Promise<TransactWriteReturn>;
  then<B extends TransactWriteExecutor>(
    other: B,
  ): TransactWriteClient<[this, B]>;
}

export class TransactWriteExecutorHolder implements TransactWriteExecutor {
  constructor(
    private readonly client: DynamoDBDocument,
    public readonly input: TransactWriteItemsInput,
    private readonly logStatements: undefined | boolean,
  ) {}

  /**
   * Execute the transactional write request
   */
  async execute(options: TransactWriteOptions): Promise<TransactWriteReturn> {
    return await new TransactWriteClient(
      this.client,
      [this],
      this.logStatements,
    ).execute(options);
  }

  /**
   * Append another set of requests to apply alongside these requests.
   * @param other
   */
  then<B extends TransactWriteExecutor>(
    other: B,
  ): TransactWriteClient<[this, B]> {
    return new TransactWriteClient(
      this.client,
      [this, other],
      this.logStatements,
    );
  }
}

export class TransactWriteClient<T extends TransactWriteExecutor[]> {
  public readonly input: TransactWriteItemsInput;

  constructor(
    private readonly client: DynamoDBDocument,
    private readonly executors: T,
    private readonly logStatements: undefined | boolean,
  ) {
    const TransactItems = this.executors.flatMap(
      (it) => it.input.TransactItems ?? [],
    );
    this.input = { TransactItems };
  }

  then<B extends TransactWriteExecutor>(
    other: B,
  ): TransactWriteClient<[...T, B]> {
    return new TransactWriteClient<[...T, B]>(
      this.client,
      [...this.executors, other],
      this.logStatements,
    );
  }

  async execute(
    options: TransactWriteOptions = {},
  ): Promise<TransactWriteReturn> {
    const input: TransactWriteItemsInput = {
      TransactItems: this.input.TransactItems,
      ReturnConsumedCapacity: options.returnConsumedCapacity,
      ReturnItemCollectionMetrics: options.returnItemCollectionMetrics,
      ClientRequestToken: options.clientRequestToken,
    };
    if (this.logStatements) {
      console.log(
        `TransactWriteCommandInput: ${JSON.stringify(input, null, 2)}`,
      );
    }
    const result = await this.client.transactWrite(input);
    return {
      consumedCapacity: result.ConsumedCapacity,
      itemCollectionMetrics: result.ItemCollectionMetrics,
    };
  }
}

export class DynamoTransactWriter<TableConfig extends TableDefinition> {
  constructor(private readonly clientConfig: DynamoConfig) {}

  put(options: TransactPutOptions<TableConfig['type']>): TransactWriteExecutor {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition && filterParts(attributeBuilder, options.condition);
    const put: Put = {
      TableName: this.clientConfig.tableName,
      Item: options.item,
      ReturnValuesOnConditionCheckFailure:
        options.returnValuesOnConditionCheckFailure,
      ...(condition ? { ConditionExpression: condition } : {}),
      ...attributeBuilder.asInput(),
    };
    const client = this.clientConfig.client;
    return new TransactWriteExecutorHolder(
      client,
      { TransactItems: [{ Put: put }] },
      this.clientConfig.logStatements,
    );
  }

  delete(options: TransactDeleteOptions<TableConfig>): TransactWriteExecutor {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition && filterParts(attributeBuilder, options.condition);
    const deleteInput: Delete = {
      TableName: this.clientConfig.tableName,
      Key: options.key,
      ReturnValuesOnConditionCheckFailure:
        options.returnValuesOnConditionCheckFailure,
      ...(condition ? { ConditionExpression: condition } : {}),
      ...attributeBuilder.asInput(),
    };
    const client = this.clientConfig.client;
    return new TransactWriteExecutorHolder(
      client,
      { TransactItems: [{ Delete: deleteInput }] },
      this.clientConfig.logStatements,
    );
  }

  conditionCheck(
    options: TransactConditionOptions<TableConfig>,
  ): TransactWriteExecutor {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition && filterParts(attributeBuilder, options.condition);
    const conditionCheck: ConditionCheck = {
      Key: options.key,
      TableName: this.clientConfig.tableName,
      ReturnValuesOnConditionCheckFailure:
        options.returnValuesOnConditionCheckFailure,
      ConditionExpression: condition,
      ...attributeBuilder.asInput(),
    };
    const client = this.clientConfig.client;
    return new TransactWriteExecutorHolder(
      client,
      { TransactItems: [{ ConditionCheck: conditionCheck }] },
      this.clientConfig.logStatements,
    );
  }

  update<KEY extends JsonPath<TableConfig['type']>>(
    options: TransactUpdateOptions<TableConfig, KEY>,
  ): TransactWriteExecutor {
    const attributeBuilder = AttributeBuilder.create();
    const condition =
      options.condition && filterParts(attributeBuilder, options.condition);
    const update: Update = {
      Key: options.key,
      TableName: this.clientConfig.tableName,
      UpdateExpression: DynamoUpdater.updateExpression(
        attributeBuilder,
        options.updates,
        options.increments,
      ),
      ReturnValuesOnConditionCheckFailure:
        options.returnValuesOnConditionCheckFailure,
      ConditionExpression: condition,
      ...attributeBuilder.asInput(),
    };
    const client = this.clientConfig.client;
    return new TransactWriteExecutorHolder(
      client,
      { TransactItems: [{ Update: update }] },
      this.clientConfig.logStatements,
    );
  }
}
