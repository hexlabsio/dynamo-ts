import { DynamoDB } from 'aws-sdk';
import { TableClient } from '../src';
import { DynamoTypeFrom } from '../src/types';
import { complexTableDefinitionFilter } from "./tables";

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoTypeFrom<typeof complexTableDefinitionFilter>;

const testTable = new TableClient(complexTableDefinitionFilter, {
  tableName: 'complexTableDefinitionFilter',
  client: dynamoClient,
  logStatements: true,
});

const itemMinimal = {
  hash: "hash1",
  string: "string required",
  number: 10,
  boolean: true
}

const itemWithOptionals = {
  hash: "hash2",
  string: "string required",
  stringOptional: "string optional",
  number: 10,
  numberOptional: 100,
  boolean: false,
  booleanOptional: true
}

const preInserts: TableType[] = [
  itemMinimal,
  itemWithOptionals
];

describe('Dynamo Nested Filter', () => {
  const TableName = 'complexTableDefinitionFilter';

  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item }).promise()),
    );
  });

  it('string top level equals', async () => {
    const result = await testTable.scan({
      filter: compare => compare().string.eq("string required")
    });
    expect(result.member[0]).toEqual(itemMinimal);
  });

  it('boolean top level equals true', async () => {
    const result = await testTable.scan({
      filter: compare => compare().boolean.eq(true)
    });
    expect(result.member[0]).toEqual(itemMinimal);
  });

  it('boolean top level equals false', async () => {
    const result = await testTable.scan({
      filter: compare => compare().boolean.eq(false)
    });
    expect(result.member).toEqual([itemWithOptionals]);
  });

  it('number top level equals', async () => {
    const result = await testTable.scan({
      filter: compare => compare().number.eq(10)
    });
    expect(result.member[0]).toEqual(itemMinimal);
  });

  it('string optional top level equals', async () => {
    const result = await testTable.scan({
      filter: compare => compare().stringOptional.eq("string optional")
    });
    expect(result.member).toEqual([itemWithOptionals]);
  });

  it('boolean optional top level equals true', async () => {
    const result = await testTable.scan({
      filter: compare => compare().booleanOptional.eq(true)
    });
    expect(result.member).toEqual([itemWithOptionals]);
  });

  // it('boolean optional top level equals false', async () => {
  //   const result = await testTable.scan({
  //     filter: compare => compare().booleanOptional.eq(false)
  //   });
  //   expect(result.member).toEqual([itemWithOptionals]);
  // });

  it('number optional top level equals', async () => {
    const result = await testTable.scan({
      filter: compare => compare().numberOptional.eq(100)
    });
    expect(result.member).toEqual([itemWithOptionals]);
  });

  it('string top level contains', async () => {
    const result = await testTable.scan({
      filter: compare => compare().string.contains("required")
    });
    expect(result.member[0]).toEqual(itemMinimal);
  });

  it('string top level exists', async () => {
    const result = await testTable.scan({
      filter: compare => compare().stringOptional.exists()
    });
    expect(result.member[0]).toEqual(itemWithOptionals);
  });

  it('string top level not exists', async () => {
    const result = await testTable.scan({
      filter: compare => compare().stringOptional.notExists()
    });
    expect(result.member[0]).toEqual(itemMinimal);
  });

  it('number top level is between', async () => {
    const result = await testTable.scan({
      filter: compare => compare().number.between(1, 20)
    });
    expect(result.member[0]).toEqual(itemMinimal);
  });

  it('number top level is not between', async () => {
    const result = await testTable.scan({
      filter: compare => compare().number.between(200, 300)
    });
    expect(result.member).toEqual([]);
  });
});
