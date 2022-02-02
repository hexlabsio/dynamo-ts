# @hexlabs/dynamo-ts

A library to make interacting with DynamoDb simpler with a typed interface.

![Typescript](https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white)
![ESLint](https://img.shields.io/badge/ESLint-8080f2?style=flat-square&logo=eslint&logoColor=white)
![Prettier](https://img.shields.io/badge/Prettier-ff69b4?style=flat-square&logo=prettier&logoColor=white)

![Version](https://img.shields.io/npm/v/@hexlabs/dynamo-ts?label=%40hexlabs%2Fdynamo-ts)

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
const result = await myTable.get({ identifier: 'id'}); // typeof result.item is {identifier: string; text: string}
```
