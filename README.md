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

> Version 6.x supports both CommonJS and ESM.

## Table of contents

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
  - [Transactional Writes](#transactional-writes)
  - [Transactional Gets](#transactional-gets)
- [Single Table Design](#single-table-design)
  - [Why single table design?](#why-single-table-design)
  - [How dynamo-ts models it](#how-dynamo-ts-models-it)
  - [Worked example: a shop](#worked-example-a-shop)
  - [Generated keys](#generated-keys)
  - [Reading and writing](#reading-and-writing)
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

## Get Started

Create a definition for your table.

> The definition holds the type information, and you can also use it to generate the table in CloudFormation, CDK, Terraform or SST (see [Infrastructure as code](#infrastructure-as-code)).

<!-- AUTO-GENERATED-CONTENT:START (CODE:src=./test/examples/define-table.ts&lines=3-100) -->
<!-- The below code snippet is automatically added from ./test/examples/define-table.ts -->
```ts
type MyTableType = { identifier: string; sort: string; abc: { xyz: number } };

export const myTableDefinition = TableDefinition.ofType<MyTableType>()
  .withPartitionKey('identifier') // <- type checked to be a key in your type
  .withSortKey('sort') // <- optional, also type checked
  .withGlobalSecondaryIndex('my-index', 'sort')
  .withNoSortKey(); // Global or Local index
```
<!-- AUTO-GENERATED-CONTENT:END -->

Build a client from the definition above:

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
};

export const myTableClient = TableClient.build(myTableDefinition, dynamoConfig);
```
<!-- The below code snippet is automatically added from ./test/examples/define-table.ts -->
<!-- AUTO-GENERATED-CONTENT:END -->

You can now use the client to talk to DynamoDB:

```typescript
// PUT ITEM
await myTableClient.put({ identifier: 'id', sort: 'a', abc: { xyz: 1 } }); // must match MyTableType

// GET ITEM
const result = await myTableClient.get({ identifier: 'id', sort: 'a' }); // must be the full key
// typeof result.item is MyTableType | undefined

// QUERY AN INDEX
await myTableClient.index('my-index').query({ sort: 'a' }); // index names are type checked too
```

# Examples

The examples below use this table of cars (also in [`examples/example-table.ts`](examples/example-table.ts)):

```typescript
type Car = { make: string; identifier: string; model: string; year: number; colour: string };

const exampleCarTable = TableDefinition.ofType<Car>()
  .withPartitionKey('make')
  .withSortKey('identifier')
  .withGlobalSecondaryIndex('model-index', 'make').withSortKey('model')
  .withGlobalSecondaryIndex('model-year-index', 'model').withSortKey('year');

const tableClient = TableClient.build(exampleCarTable, { client, tableName: 'cars' });
```

## Scan Table

```typescript
// Scan one page of the table
const { member, next } = await tableClient.scan();
// typeof member = Car[]

// Fetch the next page by passing the token back in
await tableClient.scan({ next });

// Filter results: all cars from the year 2000
await tableClient.scan({ filter: compare => compare().year.eq(2000) });

// Read every page
const { member: allCars } = await tableClient.scanAll();
```

## Get Item

```typescript
// Get Item (Partition Key and Sort Key)
const { item } = await tableClient.get({ make: 'Tesla', identifier: '1234' });
// typeof item = Car | undefined

// Get a projected item
const result = await tableClient.get(
  { make: 'Tesla', identifier: '1234' },
  { projection: projector => projector.project('model') },
);
// typeof result.item = { model: string } | undefined
```

## Put Item

```typescript
// Put Item
await tableClient.put({ identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white' });

// Put Item and return the overwritten item
const result = await tableClient.put(
  { identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white' },
  { returnValues: 'ALL_OLD' },
);
// typeof result.item = Car | undefined

// Only put the item if it doesn't already exist (throws ConditionalCheckFailedException otherwise)
await tableClient.put(
  { identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white' },
  { condition: compare => compare().identifier.notExists },
);
```

## Delete Item

```typescript
// Delete (requires the full key)
await tableClient.delete({ identifier: '1234', make: 'Tesla' });

// Delete conditionally and return what was removed
const { item } = await tableClient.delete(
  { identifier: '1234', make: 'Tesla' },
  { condition: compare => compare().colour.eq('white'), returnValues: 'ALL_OLD' },
);
```

## Query Items

`query(keys, options)` takes the key condition first and the options (filter, projection, paging and so on) second.

```typescript
// Get all cars with make 'Tesla'
await tableClient.query({ make: 'Tesla' });

// Sort key conditions: eq, lt, lte, gt, gte, between, beginsWith
await tableClient.query({ make: 'Tesla', identifier: sortKey => sortKey.beginsWith('12') });

// Query and filter: all Nissan cars from 2006
await tableClient.query({ make: 'Nissan' }, { filter: compare => compare().year.eq(2006) });

// Query an index, newest first
// All Nissan cars with a model beginning with '3'
await tableClient.index('model-index').query(
  { make: 'Nissan', model: sortKey => sortKey.beginsWith('3') },
  { scanIndexForward: false },
);

// Filter with between: all Nissan cars between 2006 and 2022
await tableClient.query({ make: 'Nissan' }, { filter: compare => compare().year.between(2006, 2022) });

// Combine comparisons with and / or / not
await tableClient.query(
  { make: 'Nissan' },
  { filter: compare => compare().year.between(2006, 2007).and(compare().colour.eq('Metallic Black')) },
);

// Projection: only return model and year
const result = await tableClient.query(
  { make: 'Tesla' },
  { projection: projector => projector.project('model').project('year') },
);
// typeof result.member = { model: string; year: number }[]

// Read every page
await tableClient.queryAll({ make: 'Tesla' });
```

## Update Items

```typescript
// Set the year to 2022 and remove the colour (undefined means REMOVE)
await tableClient.update({
  key: { identifier: '1234', make: 'Tesla' },
  updates: { year: 2022, colour: undefined },
});

// Atomic increment
// Add 1 to the year (start at 2020 if it doesn't exist) and set the model
await tableClient.update({
  key: { identifier: '1234', make: 'Tesla' },
  updates: { year: 1, model: 'Another Model' },
  increments: [{ key: 'year', start: 2020 }],
});

// Return old values
const result = await tableClient.update({
  key: { identifier: '1234', make: 'Tesla' },
  updates: { year: 2022, colour: undefined },
  return: 'ALL_OLD',
});
// typeof result.item = Car
```

Nested attributes can be updated by path, e.g. `updates: { 'abc.xyz': 9 }`.

## Multi-Table Batch Gets (With Projections)

```typescript
const result = await testTable
  .batchGet([{ identifier: '0' }, { identifier: '3' }, { identifier: '4' }])
  // Use and() to combine requests against other tables
  .and(
    testTable2.batchGet(
      [
        { identifier: '10000', sort: '0' },
        { identifier: '10008', sort: '8' },
      ],
      { projection: projector => projector.project('sort') },
    ),
  )
  .execute();
// result.items is a typed tuple, one entry per table
```

Batches larger than DynamoDB's limits (100 keys for gets, 25 items for writes) are split into multiple requests. Call
`execute(true)` on a write batch, or on a get batch combined with `and()`, to retry unprocessed keys or items with
exponential backoff. The optional second argument sets the maximum number of retries (default 10).

## Multi-Table Batch Writes

```typescript
await testTable
  // Start with batchPut or batchDelete against one table
  .batchDelete([{ identifier: 'id1' }])
  // Then use and() to combine other operations against other tables
  .and(testTable.batchPut([{ identifier: 'id2', text: 'text' }]))
  .and(testTable2.batchPut([{ identifier: 'id3', text: 'text' }]))
  .execute();
```

## Transactional Writes

```typescript
await transactionTable.transaction
  .put({
    item: { identifier: '777', count: 1, description: 'some description' },
    condition: compare => compare().description.notExists,
  })
  .then(
    transactionTable.transaction.update({
      key: { identifier: '777-000' },
      increments: [{ key: 'count', start: 0 }],
      updates: { count: 5 },
    }),
  )
  .execute();
```

`transaction.delete` and `transaction.conditionCheck` can also be chained with `.then()`.

## Transactional Gets

```typescript
const result = await transactionTable.transaction
  .get([{ identifier: '0' }])
  .and(testTable2.transaction.get([{ identifier: '10000', sort: '0' }]))
  .execute();
```

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

You describe each entity as a **part** of the table using `TablePartInfo`. Each part is defined relative to its parent.
There are three building blocks:

| Builder | Partition key | Sort key | Use it when |
|---|---|---|---|
| `TablePartInfo.from<T>().withKeys(pk, sk)` | `pk` | `sk` | Defining a root entity. |
| `parent.joinPart<T>().withKey(k)` | **same** as parent | parent's sort keys + `k` | The child is small or bounded and you want to read it **together with its parent** in one query. |
| `parent.childPart<T>().withKey(k)` | parent's partition + sort keys | `k` | The child can grow without bound, so it gets its **own partition** under the parent. |

The type passed to `joinPart<T>()` / `childPart<T>()` must include all of the parent's key attributes, and the new key
must be an attribute of `T`. The compiler checks both.

## Worked example: a shop

A store has customers. Customers have a handful of addresses (bounded, so we read them with the customer) and any
number of orders (unbounded, so each customer gets an order partition). Orders have lines that we always want with the
order.

```typescript
import { TablePartClient, TablePartInfo } from '@hexlabs/dynamo-ts';

type Customer = { store: string; customer: string; name: string };
type Address = { store: string; customer: string; address: string; city: string };
type Order = { store: string; customer: string; order: string; total: number };
type OrderLine = { store: string; customer: string; order: string; line: string; sku: string };

// Root: partition = store, sort = customer
const customers = TablePartInfo.from<Customer>().withKeys('store', 'customer');

// Joined: lives in the customer's partition, read alongside the customer
const addresses = customers.joinPart<Address>().withKey('address');

// Child: gets its own partition per customer
const orders = customers.childPart<Order>().withKey('order');

// Joined to orders: lives next to its order
const orderLines = orders.joinPart<OrderLine>().withKey('line');

const shop = TablePartClient.fromParts(
  { client: dynamoClient, tableName: 'shop' },
  customers,
  addresses,
  orders,
  orderLines,
);
```

`fromParts` returns an object with one client per part, **named after the key passed to `withKeys` / `withKey`**:
`shop.customer`, `shop.address`, `shop.order` and `shop.line`.

By default the base table has a string partition key called `partition` and a string sort key called `sort`
(exported as `defaultBaseTable`). To use different attribute names, pass your own definition:

```typescript
const baseTable = TableDefinition.ofType<{ pk: string; sk: string }>()
  .withPartitionKey('pk')
  .withSortKey('sk');

const shop = TablePartClient.fromPartsWithBaseTable(baseTable, config, customers, addresses, orders, orderLines);
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

`query(partition, sortKeys?, options?)` takes the partition identifiers, then an optional builder that narrows the
sort key from left to right, then the usual query options (`filter`, `projection`, `limit` and so on):

```typescript
// Every customer in the store (addresses share the partition but are excluded)
await shop.customer.query({ store: 'acme' });

// Every line on order o-1
await shop.line.query({ store: 'acme', customer: 'alice' }, keys => keys.order('o-1'));

// A specific line
await shop.line.query({ store: 'acme', customer: 'alice' }, keys => keys.order('o-1').line('2'));
```

## Fetching a parent with its children

`queryWithParents` reads the top-level parents in a partition along with all of their joined children, and groups them
into a tree that follows the `joinPart` chain:

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

- **Choose join or child deliberately.** A `joinPart` shares its parent's partition, which keeps reads cheap but makes
  the partition grow. A single partition is limited in throughput, and one query page is at most 1 MB. Use
  `childPart` for anything unbounded.
- **`queryWithParents` makes one query per level.** A chain of `n` parts costs at least `n` queries per page. Use `limit`
  to keep the children of a page within memory and capacity budgets.
- **Sort key narrowing is a prefix match.** `keys.order('o-1')` becomes `begins_with(sort, '#LINE#ORDER$o-1')`,
  which also matches order `o-10`. Use fixed-width or delimited identifiers (for example ULIDs or UUIDs) if this
  matters to you.
- **Avoid `#` and `$` in key values.** They are used as separators in the generated keys.
- **Part clients cover** `put`, `get`, `delete`, `batchPut`, `query`, `queryWithParents` and `queryAllWithParents`. To change an item, `put`
  it again.
- **Part names must be unique.** Clients are keyed by the final key name, so two parts can't both end in `withKey('id')`.

# Infrastructure as code

Any `TableDefinition`, including the base table for single table design, can describe its own table for your
infrastructure tool. The key schema, attribute definitions and indexes are filled in for you. Anything else (billing,
tags, streams and so on) is passed through in the tool's own format.

dynamo-ts doesn't depend on any of these tools: each helper returns a plain object (or a string for HCL).

> Key attributes are always declared as strings (`S`), because the attribute types aren't known at runtime.

## CloudFormation

Returns the properties of an `AWS::DynamoDB::Table` resource:

```typescript
import { defaultBaseTable } from '@hexlabs/dynamo-ts';

const carTableProperties = exampleCarTable.asCloudFormation('cars', { BillingMode: 'PAY_PER_REQUEST' });
const shopTableProperties = defaultBaseTable.asCloudFormation('shop', { BillingMode: 'PAY_PER_REQUEST' });
```

## CDK

Returns props for the `TableV2` construct. Pass in the `aws-cdk-lib/aws-dynamodb` module so the props use the CDK's
own enums and type-check without casts:

```typescript
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

new dynamodb.TableV2(this, 'Cars', exampleCarTable.asCdk(dynamodb, 'cars', {
  billing: dynamodb.Billing.onDemand(),
  pointInTimeRecovery: true,
}));

// Leave the name out to let CloudFormation generate one
new dynamodb.TableV2(this, 'Shop', defaultBaseTable.asCdk(dynamodb));
```

## Terraform

`asTerraformHcl` writes an `aws_dynamodb_table` resource that you can save to a `.tf` file:

```typescript
import { writeFileSync } from 'fs';

writeFileSync('cars.tf', exampleCarTable.asTerraformHcl('cars', 'cars', {
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
  resource: { aws_dynamodb_table: { cars: exampleCarTable.asTerraform('cars', { billing_mode: 'PAY_PER_REQUEST' }) } },
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
const cars = new sst.aws.Dynamo('Cars', exampleCarTable.asSst({ stream: 'new-and-old-images' }));
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

2. In **package.json**, add a `pretest` script that runs the setup file (you may need `ts-node` as a dev dependency).
   It writes `jest-dynamodb-config.js` to the project root, which is the file `@shelf/jest-dynamodb` looks for.

```json
"scripts": {
  "pretest": "ts-node ./jest-setup.ts",
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
