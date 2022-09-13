# @hexlabs/dynamo-ts

A library to make interacting with DynamoDb simpler with a typed interface.

![Typescript](https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white)
![ESLint](https://img.shields.io/badge/ESLint-8080f2?style=flat-square&logo=eslint&logoColor=white)
![Prettier](https://img.shields.io/badge/Prettier-ff69b4?style=flat-square&logo=prettier&logoColor=white)

![Version](https://img.shields.io/npm/v/@hexlabs/dynamo-ts?label=%40hexlabs%2Fdynamo-ts)


## Installation

Using npm:
```shell
$ npm i @hexlabs/dynamo-ts
```

## Get Started

Create a definition for your table

```typescript
export const simpleTableDefinition = defineTable(
  {
    identifier: 'string', // Notice the quotes around the type
    text: 'string',
  },
  'identifier', // Partition Key (This must be one of the keys defined above)
);
```

Build a client for the table

```typescript
const myTable = TableClient.build(simpleTableDefinition, {
  tableName: 'tableName',
  client: documentClient,
  logStatements: true // Logs all interactions with the table
});
```

Start interacting with the database with types

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
const executor = testTable
        //Choose batchPut or Delete to begin the operation agains an initial table
        .batchDelete({ identifier: 'id1' })
        //Then, use and() to combine other operations against other tables
        .and(testTable.batchPut([{ identifier: 'id2', text: 'text' }]))
        .and(testTable2.batchPut([{ identifier: 'id3', text: 'text' }]))
        .execute();
```
