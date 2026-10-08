# @hexlabs/dynamo-ts

![Version](https://img.shields.io/npm/v/@hexlabs/dynamo-ts?label=%40hexlabs%2Fdynamo-ts)
![Typescript](https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white)
![ESLint](https://img.shields.io/badge/ESLint-8080f2?style=flat-square&logo=eslint&logoColor=white)
![Prettier](https://img.shields.io/badge/Prettier-ff69b4?style=flat-square&logo=prettier&logoColor=white)

**DynamoDB + TypeScript made simple.**

dynamo-ts is a thin, type-safe layer over the AWS SDK v3 `DynamoDBDocument` client. You describe your table once, and
every operation (keys, filters, conditions, projections, updates) is checked against that description at compile time.
You never write an expression string or an `ExpressionAttributeNames` map by hand.

- **Typed keys.** `get`, `delete` and `update` only accept the key attributes you defined.
- **Typed expressions.** Filters, conditions and key conditions are built with a fluent, autocompleting API.
- **Typed projections.** Project `model` and `year` and the result type becomes `{ model: string; year: number }`.
- **Batches and transactions** across multiple tables. Batches are chunked to DynamoDB's limits for you, and unprocessed items can optionally be retried.
- **Single table design.** Model hierarchical entities in one table without hand-crafting `PK`/`SK` strings.
- **Infrastructure from types.** Generate tables for CloudFormation, CDK, Terraform and SST, plus local test tables, from the same definition.

> Supports both CommonJS and ESM.

## Table of contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Reference](#reference)
  - [Put](#put)
  - [Get](#get)
  - [Delete](#delete)
  - [Update](#update)
  - [Query](#query)
  - [Scan](#scan)
  - [Querying indexes](#querying-indexes)
  - [Paging through results](#paging-through-results)
  - [Filters and conditions](#filters-and-conditions)
  - [Projections](#projections)
  - [Batch operations](#batch-operations)
  - [Transactions](#transactions)
  - [Crud helper](#crud-helper)
  - [Key types and local indexes](#key-types-and-local-indexes)
  - [Errors](#errors)
- [Single Table Design](#single-table-design)
  - [Why single table design?](#why-single-table-design)
  - [How dynamo-ts models it](#how-dynamo-ts-models-it)
  - [Worked example: a shop](#worked-example-a-shop)
  - [Generated keys](#generated-keys)
  - [Reading and writing](#reading-and-writing)
  - [Transactions across parts](#transactions-across-parts)
  - [Indexes](#indexes)
    - [Local indexes](#local-indexes)
  - [Fetching a parent with its children](#fetching-a-parent-with-its-children)
    - [Paging](#paging)
  - [Things to know](#things-to-know)
- [Infrastructure as code](#infrastructure-as-code)
  - [CloudFormation](#cloudformation)
  - [CDK](#cdk)
  - [Terraform](#terraform)
  - [SST](#sst)
- [Testing](#testing)
- [Contributors](#contributors)

# Installation

```shell
npm i @hexlabs/dynamo-ts @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

# Quick start

Describe your table once:

<!-- AUTO-GENERATED-CONTENT:START (CODE:src=./test/examples/define-table.ts) -->
<!-- The below code snippet is automatically added from ./test/examples/define-table.ts -->
```ts
import { TableDefinition } from '@hexlabs/dynamo-ts';

type MyTableType = { identifier: string; sort: string; abc: { xyz: number } };

export const myTableDefinition = TableDefinition.ofType<MyTableType>()
  .withPartitionKey('identifier') // <- type checked to be a key in your type
  .withSortKey('sort') // <- optional, also type checked
  .withGlobalSecondaryIndex('my-index', 'sort')
  .withNoSortKey(); // Global or Local index
```
<!-- AUTO-GENERATED-CONTENT:END -->

Build a client from the definition:

<!-- AUTO-GENERATED-CONTENT:START (CODE:src=./test/examples/create-client.ts) -->
<!-- The below code snippet is automatically added from ./test/examples/create-client.ts -->
```ts
import { DynamoConfig, TableClient } from '@hexlabs/dynamo-ts';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { myTableDefinition } from './define-table';

const dynamoConfig: DynamoConfig = {
  client: DynamoDBDocument.from(new DynamoDB({})),
  tableName: 'my-table',
  logStatements: true, // Logs all interactions with Dynamo
};

export const myTableClient = TableClient.build(myTableDefinition, dynamoConfig);
```
<!-- AUTO-GENERATED-CONTENT:END -->

Every operation is now checked against `MyTableType`:

```typescript
await myTableClient.put({ identifier: 'id', sort: 'a', abc: { xyz: 1 } }); // the item must match MyTableType

const { item } = await myTableClient.get({ identifier: 'id', sort: 'a' }); // the full key is required
// typeof item is MyTableType | undefined
```

The [Reference](#reference) covers every operation, starting with the simple ones. For many entity types in one
table, see [Single Table Design](#single-table-design).

# Reference

The examples use this table of cars:

```typescript
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { TableClient, TableDefinition } from '@hexlabs/dynamo-ts';

type Car = {
  make: string;
  identifier: string;
  model: string;
  year: number;
  colour: string;
  specs?: { seats: number; doors: number };
  tags?: string[];
};

const carTable = TableDefinition.ofType<Car>()
  .withPartitionKey('make')
  .withSortKey('identifier')
  .withGlobalSecondaryIndex('model-index', 'make').withSortKey('model')
  .withGlobalSecondaryIndex('model-year-index', 'model').withSortKey('year', 'number');

const client = DynamoDBDocument.from(new DynamoDB({}));

const cars = TableClient.build(carTable, {
  client,
  tableName: 'cars',
  logStatements: false, // set to true to log every request
});
```

## Put

`put` writes a whole item, replacing any item with the same key.

```typescript
await cars.put({ make: 'Tesla', identifier: '1234', model: 'Model S', year: 2022, colour: 'white' });

// Return the item that was replaced
const { item } = await cars.put(
  { make: 'Tesla', identifier: '1234', model: 'Model S', year: 2023, colour: 'red' },
  { returnValues: 'ALL_OLD' },
);
// typeof item = Car | undefined

// Only write the item if it doesn't exist yet (otherwise throws ConditionalCheckFailedException)
await cars.put(
  { make: 'Tesla', identifier: '5678', model: 'Model 3', year: 2021, colour: 'blue' },
  { condition: (compare) => compare().identifier.notExists },
);
```

Options: `condition` ([conditions](#filters-and-conditions)), `returnValues` (`'NONE'` or `'ALL_OLD'`),
`returnConsumedCapacity` and `returnItemCollectionMetrics`.

## Get

`get` reads one item by its full key, and returns `undefined` if there isn't one.

```typescript
const { item } = await cars.get({ make: 'Tesla', identifier: '1234' });
// typeof item = Car | undefined

// Only read some attributes
const { item: model } = await cars.get(
  { make: 'Tesla', identifier: '1234' },
  { projection: (projector) => projector.project('model').project('year') },
);
// typeof model = { model: string; year: number } | undefined

// Read the latest write
await cars.get({ make: 'Tesla', identifier: '1234' }, { consistentRead: true });
```

Options: `projection` ([projections](#projections)), `consistentRead` and `returnConsumedCapacity`.

## Delete

`delete` removes one item by its full key.

```typescript
await cars.delete({ make: 'Tesla', identifier: '5678' });

// Only delete a red car, and return what was deleted
const { item } = await cars.delete(
  { make: 'Tesla', identifier: '1234' },
  { condition: (compare) => compare().colour.eq('red'), returnValues: 'ALL_OLD' },
);
// typeof item = Car | undefined
```

Options: `condition`, `returnValues` (`'NONE'` or `'ALL_OLD'`), `returnConsumedCapacity` and
`returnItemCollectionMetrics`.

## Update

`update` changes some attributes and leaves the rest alone. If the item doesn't exist, it's created.

```typescript
const key = { make: 'Tesla', identifier: '1234' };

// Set attributes. Undefined values are removed.
await cars.update({ key, updates: { year: 2024, colour: undefined } });

// Set a whole map, or one nested attribute by its path (the map it's in must already exist)
await cars.update({ key, updates: { specs: { seats: 5, doors: 4 } } });
await cars.update({ key, updates: { 'specs.seats': 7 } });

// Add to a number atomically, starting from 2020 if it doesn't exist yet (giving 2021)
await cars.update({ key, updates: { year: 1 }, increments: [{ key: 'year', start: 2020 }] });

// Only update if a condition holds, and return the new item
const { item } = await cars.update({
  key,
  updates: { colour: 'black' },
  condition: (compare) => compare().year.gte(2020),
  return: 'ALL_NEW',
});
// typeof item = Car
```

`return` can be `'NONE'` (the default), `'ALL_OLD'`, `'ALL_NEW'`, `'UPDATED_OLD'` or `'UPDATED_NEW'`. The last two
return only the updated attributes, typed as `Partial<Car>`. Key attributes can't be updated.

## Query

`query` reads items from one partition, optionally narrowed by the sort key. It returns one page of results
(see [paging](#paging-through-results)).

```typescript
// Every Tesla
const { member } = await cars.query({ make: 'Tesla' });
// typeof member = Car[]

// Narrow by the sort key: eq, lt, lte, gt, gte, between or beginsWith
await cars.query({ make: 'Tesla', identifier: (sortKey) => sortKey.beginsWith('12') });
await cars.query({ make: 'Tesla', identifier: (sortKey) => sortKey.between('1000', '1999') });

// Filter the results (applied after reading, so filtered items still use capacity)
await cars.query({ make: 'Tesla' }, { filter: (compare) => compare().year.gte(2020) });

// Only some attributes, in reverse sort key order, at most 10 items
const { member: models } = await cars.query(
  { make: 'Tesla' },
  { projection: (projector) => projector.project('model'), scanIndexForward: false, limit: 10 },
);
// typeof models = { model: string }[]

// Every page
await cars.queryAll({ make: 'Tesla' });
```

Options: `filter`, `projection`, `limit`, `next`, `scanIndexForward`, `consistentRead` and `returnConsumedCapacity`.

## Scan

`scan` reads the whole table, one page at a time. Prefer `query` where you can, as a scan reads (and pays for) every
item.

```typescript
const { member, next } = await cars.scan();

// Filter the results
await cars.scan({ filter: (compare) => compare().colour.eq('red') });

// Every page
const { member: allCars } = await cars.scanAll();

// Scan in parallel: run one scan per segment
await Promise.all([0, 1, 2, 3].map((segment) => cars.scanAll({ segment, totalSegments: 4 })));
```

Options: `filter`, `projection`, `limit`, `next`, `segment`, `totalSegments`, `consistentRead` and
`returnConsumedCapacity`.

## Querying indexes

`index(name)` returns a client for one of the table's indexes, with `query`, `queryAll`, `scan` and `scanAll`. Index
names and their keys are type checked.

```typescript
// Every Model S from 2020 onwards
await cars.index('model-year-index').query({ model: 'Model S', year: (year) => year.gte(2020) });

// Teslas by model name
await cars.index('model-index').query({ make: 'Tesla', model: (model) => model.beginsWith('Model') });
```

## Paging through results

`query` and `scan` return at most one page (up to 1 MB, or `limit` items). When there's more, the result has a `next`
token: pass it back to get the next page.

```typescript
let next: string | undefined;
do {
  const page = await cars.query({ make: 'Tesla' }, { limit: 25, next });
  page.member.forEach((car) => console.log(car.model));
  next = page.next;
} while (next);
```

`queryAll` and `scanAll` read every page for you.

## Filters and conditions

Filters (`query`, `scan`) and conditions (`put`, `update`, `delete`, transactions) use the same builder. It's passed
a `compare` function: call it to start a comparison on any attribute, including nested ones.

```typescript
await cars.scan({ filter: (compare) => compare().year.gt(2020) });

// Combine with and / or / not
await cars.scan({
  filter: (compare) =>
    compare().year.gt(2020).and(compare().colour.eq('red').or(compare().colour.eq('black'))),
});
await cars.scan({ filter: (compare) => compare().not(compare().colour.eq('white')) });

// Nested attributes and list elements
await cars.scan({ filter: (compare) => compare().specs.seats.gte(5) });
await cars.scan({ filter: (compare) => compare().tags[0].eq('electric') });
```

| Operator | Example |
|---|---|
| `eq`, `neq` | `compare().colour.eq('red')` |
| `lt`, `lte`, `gt`, `gte` | `compare().year.gte(2020)` |
| `between` | `compare().year.between(2018, 2022)` |
| `in` | `compare().colour.in(['red', 'blue'])` |
| `exists`, `notExists` | `compare().specs.exists` |
| `beginsWith` | `compare().model.beginsWith('Model')` |
| `contains` | `compare().tags.contains('electric')` or `compare().model.contains('S')` |
| `isType` | `compare().year.isType('number')` |
| `and`, `or` | `a.and(b)`, or `compare().and(a, b, c)` |
| `not` | `compare().not(a)` |

Values are type checked against the attribute: `compare().year.eq('2020')` doesn't compile.

## Projections

A projection reads only some attributes, and the result type follows. Projections work with `get`, `query`, `scan`,
batch gets and transactional gets.

```typescript
const { member } = await cars.query(
  { make: 'Tesla' },
  { projection: (projector) => projector.project('model').project('specs.seats') },
);
// typeof member = { model: string; specs: { seats: number } }[]
```

## Batch operations

Batch gets and writes send many requests at once, across one or more tables. Batches larger than DynamoDB's limits
(100 keys for gets, 25 items for writes) are split into several requests for you.

```typescript
// Get several items
const { items } = await cars.batchGet([
  { make: 'Tesla', identifier: '1234' },
  { make: 'Nissan', identifier: '350' },
]).execute();
// typeof items = Car[]

// Write several items: start with batchPut or batchDelete, and add more with and()
await cars
  .batchPut([{ make: 'Ford', identifier: '1', model: 'Focus', year: 2019, colour: 'grey' }])
  .and(cars.batchDelete([{ make: 'Nissan', identifier: '350' }]))
  .execute();
```

Use `and()` to combine requests, for the same table or different ones (here `ownerTable` is a client for a table of
`Owner`s). A combined batch get returns one list per request, in order, each with its own type:

```typescript
const { items: [teslas, owners] } = await cars
  .batchGet([{ make: 'Tesla', identifier: '1234' }], { projection: (projector) => projector.project('model') })
  .and(ownerTable.batchGet([{ id: 'owner-1' }]))
  .execute();
// typeof teslas = { model: string }[], typeof owners = Owner[]
```

DynamoDB can leave some requests unprocessed when it's busy. Call `execute(true)` on a write batch, or on a combined
get batch, to retry them with exponential backoff; the optional second argument sets the maximum number of retries
(default 10).

## Transactions

A transaction applies several writes, across one or more tables, all or nothing. If any condition fails, nothing is
written and it throws `TransactionCanceledException`.

```typescript
await cars.transaction
  .put({
    item: { make: 'Tesla', identifier: '9999', model: 'Cybertruck', year: 2024, colour: 'steel' },
    condition: (compare) => compare().identifier.notExists,
  })
  .then(cars.transaction.update({ key: { make: 'Tesla', identifier: '1234' }, updates: { colour: 'blue' } }))
  .then(cars.transaction.delete({ key: { make: 'Ford', identifier: '1' } }))
  .then(
    cars.transaction.conditionCheck({
      key: { make: 'Nissan', identifier: '350' },
      condition: (compare) => compare().identifier.notExists,
    }),
  )
  .execute();
```

Transactional gets read several items at a consistent point in time. The result has one entry per key, in order:

```typescript
const { items: [tesla, ford] } = await cars.transaction
  .get([{ make: 'Tesla', identifier: '1234' }])
  .and(cars.transaction.get([{ make: 'Ford', identifier: '1' }]))
  .execute();
// typeof tesla = Car | undefined
```

## Crud helper

`Crud` wraps a client for tables keyed by a single generated id. `create` adds a random UUID as the partition key.

```typescript
import { Crud } from '@hexlabs/dynamo-ts';

type User = { id: string; name: string };
const users = new Crud(
  TableClient.build(TableDefinition.ofType<User>().withPartitionKey('id'), { client, tableName: 'users' }),
);

const user = await users.create({ name: 'Ada' }); // { id: '<uuid>', name: 'Ada' }
await users.read({ id: user.id }); // User | undefined
await users.readMany([{ id: user.id }]); // User[]
await users.readAll(); // every user
await users.update({ key: { id: user.id }, updates: { name: 'Ada Lovelace' } }); // returns the updated User
await users.deleteItem({ id: user.id });
```

## Key types and local indexes

Key attributes that aren't strings must say what type they are, because TypeScript types aren't available at
runtime. The compiler requires it, and only accepts the matching type:

```typescript
type Reading = { sensor: string; time: number; data: Buffer; recorded: number };

TableDefinition.ofType<Reading>()
  .withPartitionKey('sensor') // string: no type needed
  .withSortKey('time', 'number') // leaving out 'number' (or passing 'string') is a compile error
  .withGlobalSecondaryIndex('by-data', 'data', 'binary')
  .withNoSortKey()
  .withLocalSecondaryIndex('by-recorded')
  .withSortKey('recorded', 'number');
```

Attributes whose type could be more than one of these (such as `string | number`), or that are objects or arrays,
can't be keys.

Local secondary indexes always use the table's partition key, so they only take a name and a sort key, and need a
table with a sort key. They support strongly consistent reads, but can only be created with the table; see
[local indexes](#local-indexes) for their limitations.

## Errors

dynamo-ts doesn't wrap AWS errors, so they're thrown as the AWS SDK throws them. The common ones are:

- `ConditionalCheckFailedException` when a `put`, `update` or `delete` condition isn't met;
- `TransactionCanceledException` when a transaction is cancelled, for example because one of its conditions failed;
- `ValidationException` for requests DynamoDB rejects, such as an item missing a key attribute.

# Single Table Design

> For background, see [Typesafe DynamoDB with TypeScript](https://medium.com/hexlabs/typesafe-dynamodb-with-typescript-f0fe538186a7) on Medium.

## Why single table design?

DynamoDB has no joins. To read related data efficiently, you store the related entities **in the same partition**,
next to each other, so one `Query` returns all of them.

Single table design puts every entity type (customers, orders, order lines and so on) into **one table** with generic key
attributes (for example `partition` and `sort`). Each item's keys are built from its identifiers so that:

- items that are read together share a **partition key**, and
- the **sort key** says what kind of item it is and where it sits in the hierarchy, so `begins_with` can select one
  entity type or one sub-tree.

```
partition                    sort                                 attributes
---------------------------  -----------------------------------  ----------------------
#STORE$acme                  #CUSTOMER$alice                      name=Alice
#STORE$acme                  #ADDRESS#CUSTOMER$alice#ADDRESS$home city=Belfast
#STORE$acme#CUSTOMER$alice   #ORDER$o-1                           total=30
#STORE$acme#CUSTOMER$alice   #LINE#ORDER$o-1#LINE$1               sku=socks
#STORE$acme#CUSTOMER$alice   #LINE#ORDER$o-1#LINE$2               sku=shoes
```

This gives you fewer round trips, predictable performance and one table to provision and secure. The cost is that you
need to know your access patterns up front, and that building and parsing composite key strings by hand is fiddly and
easy to get wrong.

**dynamo-ts generates those keys for you from your types**, so you only work with plain identifiers like
`{ store, customer, order }`.

## How dynamo-ts models it

You describe the table as a tree with `TableDefinition.singleTable`. Each entity is a **part**, and each property
name is both the part's name and the attribute used as its key. Nesting a part inside another makes it a child of that
part. There are three building blocks:

| Builder | Partition key | Sort key | Use it when |
|---|---|---|---|
| `name: part<T>().partitionedBy(pk)` | `pk` | `name` | Defining a root entity. |
| `name: join<T>()` | **same** as parent | parent's sort keys + `name` | The child is small or bounded and you want to read it **together with its parent** in one query. |
| `name: child<T>()` | parent's partition + sort keys | `name` | The child can grow without bound, so it gets its **own partition** under the parent. |

Nest a part's children with `.with({ ... })`, and add it to [indexes](#indexes) with `.index(...)`. The compiler checks
that:

- each name and partition key is an attribute of the part's type;
- each part's type has all of its parent's key attributes.

If two parts have the same name, defining the table throws an error.

## Worked example: a shop

A store has customers. Customers have a handful of addresses (bounded, so we read them with the customer) and any
number of orders (unbounded, so each customer gets an order partition). Orders have lines that we always want with the
order.

```typescript
import { TableDefinition } from '@hexlabs/dynamo-ts';

type Customer = { store: string; customer: string; name: string };
type Address = { store: string; customer: string; address: string; city: string };
type Order = { store: string; customer: string; order: string; total: number };
type OrderLine = { store: string; customer: string; order: string; line: string; sku: string };

export const shopTable = TableDefinition.singleTable(({ part, join, child }) => ({
  // Root: partition = store, sort = customer
  customer: part<Customer>().partitionedBy('store').with({
    // Joined: lives in the customer's partition, read alongside the customer
    address: join<Address>(),
    // Child: gets its own partition per customer
    order: child<Order>().with({
      // Joined to orders: lives next to its order
      line: join<OrderLine>(),
    }),
  }),
}));

const shop = shopTable.client({ client, tableName: 'shop' });
```

`client` returns an object with one client per part, keyed by part name: `shop.customer`, `shop.address`, `shop.order`
and `shop.line`.

`shopTable` is a normal `TableDefinition`, so you can also use it to [generate the table](#infrastructure-as-code) or
to [create test tables](#testing).

By default the table has a string partition key called `partition` and a string sort key called `sort`. To use
different attribute names, pass options first:

```typescript
export const shopTable = TableDefinition.singleTable({ partitionKey: 'pk', sortKey: 'sk' }, ({ part, join, child }) => ({
  customer: part<Customer>().partitionedBy('store').with({ /* ... */ }),
}));
```

## Generated keys

Each part builds its keys as `#NAME$value` segments. Joined parts also put their own name at the front of the sort key,
so different entity types in the same partition can be told apart with `begins_with`:

| Part | partition | sort |
|---|---|---|
| `customer` | `#STORE$acme` | `#CUSTOMER$alice` |
| `address` | `#STORE$acme` | `#ADDRESS#CUSTOMER$alice#ADDRESS$home` |
| `order` | `#STORE$acme#CUSTOMER$alice` | `#ORDER$o-1` |
| `line` | `#STORE$acme#CUSTOMER$alice` | `#LINE#ORDER$o-1#LINE$1` |

The generated keys are stored on the item alongside your own attributes, and `put`, `get` and `delete` also return them
as `keys`.

## Reading and writing

```typescript
// Put: pass the plain entity, keys are generated
const { keys } = await shop.order.put({ store: 'acme', customer: 'alice', order: 'o-1', total: 30 });
// keys = { partition: '#STORE$acme#CUSTOMER$alice', sort: '#ORDER$o-1' }

// Get / delete: pass the identifiers only (type checked)
const { item } = await shop.customer.get({ store: 'acme', customer: 'alice' });
await shop.address.delete({ store: 'acme', customer: 'alice', address: 'home' });

// Batch writes across parts, combined with and()
await shop.line
  .batchPut([
    { store: 'acme', customer: 'alice', order: 'o-1', line: '1', sku: 'socks' },
    { store: 'acme', customer: 'alice', order: 'o-1', line: '2', sku: 'shoes' },
  ])
  .and(shop.customer.batchPut([{ store: 'acme', customer: 'bob', name: 'Bob' }]))
  .execute();
```

`update(key, options)` changes some attributes and leaves the rest alone, creating the item if it doesn't exist.
Undefined values are removed, and `condition`, `increments` and `return` work as they do on a [table
client](#update). Key attributes can't be updated, since they decide where the item lives.

```typescript
await shop.customer.update({ store: 'acme', customer: 'alice' }, { updates: { name: 'Alice Smith' } });

// Atomic increment, starting at 0, returning the new item
const { item } = await shop.order.update(
  { store: 'acme', customer: 'alice', order: 'o-1' },
  { updates: { total: 5 }, increments: [{ key: 'total', start: 0 }], return: 'ALL_NEW' },
);
```

`query(partition, sortKeys?, options?)` takes the partition identifiers, then an optional builder that narrows the
sort key from left to right, then the usual query options (`filter`, `projection`, `limit` and so on). Matching is
precise: a partial key matches whole segments, so order `o-1` doesn't match `o-10`, and a full key uses equality.

```typescript
// Every customer in the store (addresses share the partition but are excluded)
await shop.customer.query({ store: 'acme' });

// Every line on order o-1
await shop.line.query({ store: 'acme', customer: 'alice' }, keys => keys.order('o-1'));

// A specific line
await shop.line.query({ store: 'acme', customer: 'alice' }, keys => keys.order('o-1').line('2'));
```

Part clients also have the other operations you'd expect from a [table client](#reference). Keys are typed from the
part, so a number key takes a number:

```typescript
// Every page
await shop.line.queryAll({ store: 'acme', customer: 'alice' });

// Batch gets and deletes, combined across parts with and()
const { items: [customers, orders] } = await shop.customer
  .batchGet([{ store: 'acme', customer: 'alice' }])
  .and(shop.order.batchGet([{ store: 'acme', customer: 'alice', order: 'o-1' }]))
  .execute();
await shop.address.batchDelete([{ store: 'acme', customer: 'alice', address: 'home' }]).execute();

// Scan the table for one part's items (reads, and pays for, the whole table)
const { member: everyOrder } = await shop.order.scanAll({ filter: (compare) => compare().total.gt(20) });
```

## Transactions across parts

Each part client has a `transaction` property with the same requests as a [table client's](#transactions): `put`,
`update`, `delete`, `conditionCheck` and `get`. Keys are generated, index keys are maintained, and `update` follows
the same rules as above. Requests from different parts, and from other tables, combine into one transaction:

```typescript
// Create an order with its lines, only if the customer exists. All or nothing.
await shop.order.transaction
  .put({ item: { store: 'acme', customer: 'alice', order: 'o-2', total: 25 } })
  .then(shop.line.transaction.put({ item: { store: 'acme', customer: 'alice', order: 'o-2', line: '1', sku: 'hat' } }))
  .then(shop.line.transaction.put({ item: { store: 'acme', customer: 'alice', order: 'o-2', line: '2', sku: 'scarf' } }))
  .then(
    shop.customer.transaction.conditionCheck({
      key: { store: 'acme', customer: 'alice' },
      condition: (compare) => compare().customer.exists,
    }),
  )
  .execute();

// Read a customer and an order at the same point in time
const { items: [customer, order] } = await shop.customer.transaction
  .get([{ store: 'acme', customer: 'alice' }])
  .and(shop.order.transaction.get([{ store: 'acme', customer: 'alice', order: 'o-2' }]))
  .execute();
// typeof customer = Customer | undefined, typeof order = Order | undefined
```

A transaction can include up to 100 requests, and can't touch the same item twice.

## Indexes

Parts can add themselves to global and [local](#local-indexes) secondary indexes to support other access patterns.
The table's indexes come from the parts, so there's nothing else to declare.

```typescript
type Store = { org: string; store: string; zone: string; status?: 'open' | 'closed'; name: string };
type Employee = { org: string; store: string; employee: string; role: string };

export const storeTable = TableDefinition.singleTable(({ part, child }) => ({
  store: part<Store>()
    .partitionedBy('org')
    .index('byZone', { partition: [], sort: ['zone', 'status'] })
    .with({
      employee: child<Employee>().index('byRole', { partition: ['role'], sort: ['employee'] }),
    }),
}));

const stores = storeTable.client({ client, tableName: 'stores' });
```

`.index(name, { partition, sort })` lists the attributes that make up the index keys, in order. The index partition key
always starts with the part name, so `partition: []` means "every store". `sort` is optional.

| Item | `byZone_partition` | `byZone_sort` |
|---|---|---|
| Store `s1` in zone `AMER`, open | `#STORE` | `#ZONE$AMER#STATUS$open` |
| Store `s4` in zone `AMER`, no status | *(not in the index)* | |

`put` and `batchPut` write the index keys for you. If any attribute an index uses is missing, the item is left out of
that index (a sparse index). Putting the item again recomputes its index keys.

`update` keeps index keys up to date too, without reading the item first. Because of that, changing an attribute an
index uses means providing the rest of that index's attributes in the same update (attributes in the item's key are
already known). The compiler and the client both check this:

```typescript
await stores.store.update({ org: 'walmart', store: 's1' }, { updates: { zone: 'EMEA', status: 'closed' } }); // ✓
await stores.store.update({ org: 'walmart', store: 's1' }, { updates: { status: 'closed' } }); // ✗ byZone also needs zone
await stores.employee.update({ org: 'walmart', store: 's1', employee: 'e1' }, { updates: { role: 'manager' } }); // ✓ employee is in the key
await stores.store.update({ org: 'walmart', store: 's1' }, { updates: { zone: 'AMER', status: undefined } }); // ✓ leaves byZone
```

Setting an index attribute to undefined removes the item from that index. Index attributes can't be incremented,
because their new value isn't known until DynamoDB applies it.

Query an index through the part, narrowing the sort key from left to right just like `query`:

```typescript
await stores.store.index('byZone').query({});                                            // every store with a zone and status
await stores.store.index('byZone').query({}, (keys) => keys.zone('AMER'));                 // in AMER (not AMERICA)
await stores.store.index('byZone').query({}, (keys) => keys.zone('AMER').status('open'));  // open stores in AMER
await stores.employee.index('byRole').query({ role: 'manager' }, (keys) => keys.employee('e1'), { limit: 10 });
```

Index names, attributes and query keys are all type checked, and several parts can share one index with different key
layouts. Each index uses attributes called `<index>_partition` and `<index>_sort` unless you configure them:

```typescript
TableDefinition.singleTable(
  { indexes: { byRole: { partitionKey: 'gsi1pk', sortKey: 'gsi1sk' } } },
  ({ part, child }) => ({ /* ... */ }),
);
```

Defining the table throws if parts sharing an index disagree on whether it has a sort key, if a configured index isn't
used by any part, or if two key attributes have the same name.

### Local indexes

A local index keeps the table's partition key and sorts it by different attributes, so it can read a partition in
another order. Unlike global indexes, local indexes support strongly consistent reads.

```typescript
type Account = { tenant: string; account: string; name: string };
type Invoice = { tenant: string; account: string; invoice: string; due: string; status?: 'open' | 'paid' };

export const billingTable = TableDefinition.singleTable(({ part, join }) => ({
  account: part<Account>()
    .partitionedBy('tenant')
    .with({
      invoice: join<Invoice>()
        .localIndex('byDue', { sort: ['due'] })
        .localIndex('byStatus', { sort: ['status', 'due'] }),
    }),
}));

const billing = billingTable.client({ client, tableName: 'billing' });

// A tenant's invoices by due date, including ones just written
await billing.invoice.index('byDue').query({ tenant: 't1' }, undefined, { consistentRead: true });

// Open invoices, by due date
await billing.invoice.index('byStatus').query({ tenant: 't1' }, (keys) => keys.status('open'));
```

Local index queries take the part's own partition attributes. The index sort key starts with the part name (e.g.
`#INVOICE#DUE$2026-10-01`), so parts sharing a partition can share a local index without seeing each other's items.
`put`, `update` and sparse indexes work the same way as for global indexes. Local indexes use a `<index>_sort`
attribute, which you can configure with `indexes: { byDue: { sortKey: 'lsi1' } }`.

Local indexes come with real limitations, so prefer global indexes unless you need strongly consistent reads:

- **They can only be created with the table.** Adding a local index to a part later means creating a new table and
  moving the data.
- **They can't cross partitions.** They re-sort a partition, so access patterns like "every open invoice" need a global
  index.
- **Each partition key value is limited to 10 GB** across the table and its local indexes, and its throughput can't be
  spread over more than one DynamoDB partition. Single table design often puts many items in one partition, so check
  this before using them.
- **A table can have at most 5.** Defining the table throws if parts use more.

## Fetching a parent with its children

`queryWithParents` reads the top-level parents in a partition along with all of their joined children, and groups them
into a tree that follows the chain of `join` parts:

```typescript
const { member } = await shop.line.queryWithParents({ store: 'acme', customer: 'alice' });
// [
//   {
//     item: { store: 'acme', customer: 'alice', order: 'o-1', total: 30, ... },
//     member: [
//       { store: 'acme', customer: 'alice', order: 'o-1', line: '1', sku: 'socks', ... },
//       { store: 'acme', customer: 'alice', order: 'o-1', line: '2', sku: 'shoes', ... },
//     ],
//   },
// ]

const customersWithAddresses = await shop.address.queryWithParents({ store: 'acme' });
// [{ item: Customer, member: Address[] }, ...]
```

The result is typed: `{ item: Order; member: OrderLine[] }[]`. Deeper join chains nest further.

### Paging

Paging works on the **top-level parents**. `limit` caps how many parents are read per page, and `next` continues from
the previous page. Each page always contains the complete set of children for the parents it returns, so a parent and
its children are never split across pages.

```typescript
let next: string | undefined;
do {
  const page = await shop.line.queryWithParents({ store: 'acme', customer: 'alice' }, { limit: 10, next });
  page.member.forEach(({ item: order, member: lines }) => console.log(order.order, lines.length));
  next = page.next;
} while (next);
```

To read every page in one call, use `queryAllWithParents`. It accepts the same options apart from `limit` and `next`:

```typescript
const { member: orders } = await shop.line.queryAllWithParents({ store: 'acme', customer: 'alice' });
```

Under the hood this runs one query for a page of parents, then one query per level of the join chain. Each of those
queries is bounded to the sort key range of the parents on the page, and the results are grouped in memory. The
`consumedCapacity` returned is the total across all of these queries. `filter` and `projection` apply at every level, so
make sure a projection keeps the key attributes that the grouping relies on.

## Things to know

- **Choose join or child deliberately.** A `join` part shares its parent's partition, which keeps reads cheap but makes
  the partition grow. A single partition is limited in throughput, and one query page is at most 1 MB. Use
  `child` for anything unbounded.
- **`queryWithParents` makes one query per level.** A chain of `n` parts costs at least `n` queries per page. Use `limit`
  to keep the children of a page within memory and capacity budgets.
- **Avoid `#` and `$` in key values.** They are used as separators in the generated keys.
- **Key values are stored as strings.** Numbers in keys and indexes sort as text, so `10` comes before `9`. Pad them
  (e.g. `009`) if their order matters.
- **Part clients cover** `put`, `update`, `get`, `delete`, `batchGet`, `batchPut`, `batchDelete`, `query`, `queryAll`,
  `scan`, `scanAll`, `index(...).query` and `queryAll`, `queryWithParents`, `queryAllWithParents` and `transaction`.
- **Part names must be unique across the whole tree.** Clients are keyed by part name, so two parts can't both be
  called `id`, even in different branches.

# Infrastructure as code

Any `TableDefinition`, including a [single table](#single-table-design) definition, can describe its own table for your
infrastructure tool. The key schema, attribute definitions and indexes are filled in for you. Anything else (billing,
tags, streams and so on) is passed through in the tool's own format.

dynamo-ts doesn't depend on any of these tools: each helper returns a plain object (or a string for HCL).

> Key attributes are declared with the types given in the definition (see [key types](#key-types-and-local-indexes)), so number and
> binary keys get `N` and `B`. Single table keys are always strings.

## CloudFormation

Returns the properties of an `AWS::DynamoDB::Table` resource:

```typescript
const carTableProperties = carTable.asCloudFormation('cars', { BillingMode: 'PAY_PER_REQUEST' });
const shopTableProperties = shopTable.asCloudFormation('shop', { BillingMode: 'PAY_PER_REQUEST' });
```

## CDK

Returns props for the `TableV2` construct. Pass in the `aws-cdk-lib/aws-dynamodb` module so the props use the CDK's
own enums and type-check without casts:

```typescript
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

new dynamodb.TableV2(this, 'Cars', carTable.asCdk(dynamodb, 'cars', {
  billing: dynamodb.Billing.onDemand(),
  pointInTimeRecovery: true,
}));

// Leave the name out to let CloudFormation generate one
new dynamodb.TableV2(this, 'Shop', shopTable.asCdk(dynamodb));
```

## Terraform

`asTerraformHcl` writes an `aws_dynamodb_table` resource that you can save to a `.tf` file:

```typescript
import { writeFileSync } from 'fs';

writeFileSync('cars.tf', carTable.asTerraformHcl('cars', 'cars', {
  billing_mode: 'PAY_PER_REQUEST',
  tags: { team: 'cars' },
  point_in_time_recovery: { enabled: true },
}));
```

```hcl
resource "aws_dynamodb_table" "cars" {
  name = "cars"
  billing_mode = "PAY_PER_REQUEST"
  ...
  hash_key = "make"
  range_key = "identifier"
  attribute {
    name = "make"
    type = "S"
  }
  ...
  global_secondary_index {
    name = "model-index"
    key_schema {
      attribute_name = "make"
      key_type = "HASH"
    }
    ...
  }
}
```

`asTerraform` returns the same arguments as an object, for `.tf.json` files or CDKTF:

```typescript
const tfJson = {
  resource: { aws_dynamodb_table: { cars: carTable.asTerraform('cars', { billing_mode: 'PAY_PER_REQUEST' }) } },
};
```

- Extra arguments use the provider's snake_case names. Objects become nested blocks (apart from `tags`, which is a map).
- With `billing_mode = "PROVISIONED"`, the `read_capacity` and `write_capacity` you pass are copied to each global
  index, as the provider requires.
- Global indexes use `key_schema` blocks, so you need a recent AWS provider (6.x). The older `hash_key`/`range_key`
  index arguments are deprecated.
- The output isn't aligned. Run `terraform fmt` if you want the usual formatting.

## SST

Returns args for the SST v3 `sst.aws.Dynamo` component:

```typescript
const table = new sst.aws.Dynamo('Cars', carTable.asSst({ stream: 'new-and-old-images' }));
```

# Testing

We use [@shelf/jest-dynamodb](https://github.com/shelfio/jest-dynamodb) to
run a local DynamoDB during tests, and dynamo-ts can generate its table config from your definitions:

1. Create a file called `jest-setup.ts`:

```typescript
import { writeJestDynamoConfig } from '@hexlabs/dynamo-ts';
import { table1, table2 } from './test/tables';

writeJestDynamoConfig(
  { testTable: table1, ThisIsTheTableNameForTable2: table2 }, // table name -> definition
  'jest-dynamodb-config.js',
  { port: 5001 },
);
```

2. In **package.json**, add a `pretest` script that runs the setup file (for example with [`tsx`](https://tsx.is), installed as a dev dependency).
   It writes `jest-dynamodb-config.js` to the project root, which is the file `@shelf/jest-dynamodb` looks for.

```json
"scripts": {
  "pretest": "tsx ./jest-setup.ts",
  ...
}
```

3. In your tests, create a document client that points at the local instance:

```typescript
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);
```

4. Inject that client wherever you use DynamoDB, and your tests will run against tables that match your definitions.

# Contributors

Thanks to everyone who has contributed so far!

<a href="https://github.com/hexlabsio/dynamo-ts/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=hexlabsio/dynamo-ts"/>
</a>
