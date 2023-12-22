# @hexlabs/dynamo-ts

![Version](https://img.shields.io/npm/v/@hexlabs/dynamo-ts?label=%40hexlabs%2Fdynamo-ts)

> Note: Versions 5.x + are now ES Modules. If you need CommonJS the latest version using it is 4.x

DynamoDB + TypeScript made simple

![Typescript](https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white)
![ESLint](https://img.shields.io/badge/ESLint-8080f2?style=flat-square&logo=eslint&logoColor=white)
![Prettier](https://img.shields.io/badge/Prettier-ff69b4?style=flat-square&logo=prettier&logoColor=white)


<!-- AUTO-GENERATED-CONTENT:START (TOC) -->
- [Installation](#installation)
- [Get Started](#get-started)
- [Examples](#examples)
- [Scan Table](#scan-table)
- [Get Item](#get-item)
- [Put Item](#put-item)
- [Delete Item](#delete-item)
- [Query Items](#query-items)
- [Update Items](#update-items)
- [Multi-Table Batch Gets (With Projections)](#multi-table-batch-gets-with-projections)
- [Multi-Table Batch Writes](#multi-table-batch-writes)
- [Testing](#testing)
- [Contributors](#contributors)
<!-- AUTO-GENERATED-CONTENT:END -->


## Installation

Using npm:
```shell
$ npm i -S @hexlabs/dynamo-ts
```

## Get Started

Create a definition for your table

 > This can be stored and used for type information and generation in CloudFormation for example.

<!-- AUTO-GENERATED-CONTENT:START (CODE:src=./test/examples/define-table.ts&lines=3-100) -->
<!-- The below code snippet is automatically added from ./test/examples/define-table.ts -->
```ts
type MyTableType = { identifier: string; sort: string; abc: { xyz: number } };

export const myTableDefinition = TableDefinition.ofType<MyTableType>()
  .withPartitionKey('identifier') // <- type checked to be a key in your type
  .withSortKey('sort') // <- optional, aso type checked
  .withGlobalSecondaryIndex('my-index', 'sort').withNoSortKey() // Global or Local index
```
<!-- AUTO-GENERATED-CONTENT:END -->

Build a client from the definition above

<!-- AUTO-GENERATED-CONTENT:START (CODE:src=./test/examples/create-client.ts&lines=2-100) -->
<!-- The below code snippet is automatically added from ./test/examples/create-client.ts -->
```ts
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { myTableDefinition } from './define-table';

const dynamoConfig: DynamoConfig = {
  client: DynamoDBDocument.from(new DynamoDB({})),
  tableName: 'my-table',
  logStatements: true, // Logs all interactions with Dynamo
}

const myTableClient = TableClient.build(myTableDefinition, dynamoConfig);
```
<!-- The below code snippet is automatically added from ./test/examples/define-table.ts -->
<!-- AUTO-GENERATED-CONTENT:END -->

This client can now be used to interact with DynamoDb

```typescript
// PUT ITEM
await myTable.put({ identifier: 'id', text: 'some text' }); // This object must match the definition above
// GET ITEM
const result = await myTable.get({ identifier: 'id'}); 
// typeof result.item is {identifier: string; text: string}
```


## Examples

All examples can be found in the `examples` directory in this repository.

An example table for using these examples  can be found in `examples/example-table.ts`

## Scan Table

```typescript
// Scan Table
const {member, next} = await tableClient.scan();
// typeof member = {identifier: string; make: string; model: string; year: number; colour: string}[]
// use next to paginate by passing in as argument to scan

// Filter results
// Get all cars in the year 2000
await tableClient.scan({filter: compare => compare().year.eq(2000)});
````
## Get Item

```typescript
// Get Item (Partition Key and Sort Key)
await tableClient.get({make: 'Tesla', identifier: '<identifier>'});

// Get Projected Item
const result = await tableClient.get({identifier: '1234', make: 'Tesla'}, {projection: projector => projector.project('model')});
// typeof result = {model: string} | undefined;
```

## Put Item

```typescript
// Put Item
await tableClient.put({identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white'});

//Put Item Return overwritten item
const result = await tableClient.put(
    {identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white'},
    {returnOldValues: true}
);
// typeof result.item = {identifier: string; make: string; model: string; year: number; colour: string}

// Conditionally Put Item if doesn't already exist (throws ConditionError)
await tableClient.put(
    {identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white'},
    {condition: compare => compare().notExists('identifier')}
)
```

## Delete Item
```typescript
//Delete (requires Partition Key and Sort Key)
await tableClient.delete({identifier: '1234', make: 'Tesla'})
```

## Query Items

```typescript
// Simple Query against Partition
// Get all Cars with make 'Tesla'
await tableClient.query({make: 'Tesla'});

// Query and Filter
// Get all Nissan Cars in the year 2006
await tableClient.query({make: 'Nissan', filter: compare => compare().year.eq(2006)});

// Query an Index using KeyConditionExpression
// Get all Nissan Cars with a model beginning with '3' and order backwards
await tableClient.index('model-index').query({
    make: 'Nissan',
    model: sortKey => sortKey.beginsWith('3'),
    dynamo: { ScanIndexForward: false }
});

// Filter with between
// Get all Nissan Cars between 2006 and 2022
await tableClient.query({make: 'Nissan', filter: compare => compare().year.between(2006, 2022)});

// Combining Filter Comparisons (and / or)
// Get all Nissan Cars between 2006 & 2007 AND with colour 'Metallic Black'
await tableClient.query({
    make: 'Nissan',
    filter: compare => compare().year.between(2006, 2007).and(compare().colour.eq('Metallic Black'))
});

// Projection
// Get only model and year
const result = await tableClient.query({make: 'Tesla', projection: projector => projector.project('model').project('year')});
// typeof result.member = {model: string; year: string}
```

# Update Items
```typescript
//Update Model S Tesla by setting the year to 2022 and deleting the colour (undefined means delete)
await tableClient.update({key: {identifier: '1234', make: 'Tesla'}, updates: {year: 2022, colour: undefined}});

//Atomic Addition
//Update by incrementing the year by 1 atomically, if it doesn't exist set it to 2020, also set model to 'Another Model'
await tableClient.update({key: {identifier: '1234', make: 'Tesla'}, updates: {year: 1, model: 'Another Model'}, increments: [{key: 'year', start: 2020}]});

//Return Old Values
const result = await tableClient.update({key: {identifier: '1234', make: 'Tesla'}, updates: {year: 2022, colour: undefined}, return: 'ALL_OLD'});
// typeof result.item = {identifier: string; make: string; model: string; year: number; colour: string}
```

# Multi-Table Batch Gets (With Projections)
```typescript
const result = await testTable
        .batchGet([
          { identifier: '0' },
          { identifier: '3' },
          { identifier: '4' },
        ])
        //Use and() to combine other operations against other tables
        .and(
          testTable2.batchGet(
            [
              { identifier: '10000', sort: '0' },
              { identifier: '10008', sort: '8' },
            ],
            { projection: (projector) => projector.project('sort') },
          ),
        )
        .execute();
```

# Multi-Table Batch Writes 
```typescript
const result = await testTable
        //Choose batchPut or Delete to begin the operation agains an initial table
        .batchDelete({ identifier: 'id1' })
        //Then, use and() to combine other operations against other tables
        .and(testTable.batchPut([{ identifier: 'id2', text: 'text' }]))
        .and(testTable2.batchPut([{ identifier: 'id3', text: 'text' }]))
        .execute();
```

# Transactional Writes
```typescript
const result = await transactionTable
    .transaction
    .put({
      item: { identifier: '777', count: 1, description: 'some description' },
      condition: compare => compare().description.notExists
    })
    .then(
      transactionTable.transaction.update({
        key: { identifier: '777-000' },
        increments: [{key: 'count', start: 0}],
        updates: { count: 5 }
      })
    )
  .execute();
```

# Transactional Gets
```typescript
const result = await transactionTable
  .transaction.get([{identifier: '0'}])
    .and(
      testTable2.transaction.get([{identifier: '10000', sort: '0'}])
    ).execute()
```

# Testing
Testing is no different than how you would have tested dynamo before. We use @shelf/jest-dynamodb to run a local version of dynamodb when we test.
If you would like us to generate table definitions that can be used in this testing library, do the following:

1. Create a file called jest-setup.ts

```typescript
import {table1, table2}  from './test/tables';
import {writeJestDynamoConfig} from "./src/dynamo-jest-setup";

(async () => writeJestDynamoConfig({testTable: table1, 'ThisIsTheTableNameForTable2': table2}, 'jest-dynamodb-config.js',{port: 5001}))();
```

2. Then, in **package.json**, Update your scripts to include a pretest command which executes the setup file. Note that you may need to install ts-node as a dev dependency.

This will create a file named `jest-dynamodb-config.js` at the root of the project which is the config file searched for by the testing library to build tables.

```json
"scripts": {
  "pretest": "ts-node ./jest-setup.ts",
  ...
}
```

3. At the top of the test file you want to use dynamo in add the following to get a document client:

```typescript
import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/'  },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' }
});
const dynamoClient = DynamoDBDocument.from(dynamo);
```

4. Inject the client wherever you use dynamo, and you will have tables that match your dynamo definitions.

# Contributors
Thanks to everyone who has contributed so far!

<a href="https://github.com/hexlabsio/dynamo-ts/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=hexlabsio/dynamo-ts"/>
</a>
