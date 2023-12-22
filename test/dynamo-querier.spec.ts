import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { CompareWrapperOperator, Operation, TableClient } from '../src';
import { DynamoQuerier, QuerierReturn } from '../src';
import {
  ComplexTable2,
  complexTableDefinitionQuery,
  IndexTable,
  indexTableDefinition,
  SimpleTable2,
  simpleTableDefinition2,
  sortKeyAsIndexPartitionKeyTableDefinition,
} from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

const TableName = 'complexTableDefinitionQuery';
const TableName2 = 'simpleTableDefinition2';
const TableName3 = 'indexTableDefinition';
const SortKeyAsIndexPartitionKeyTableName =
  'sortKeyAsIndexPartitionKeyTableDefinition';

const testTable = new DynamoQuerier(complexTableDefinitionQuery, {
  tableName: TableName,
  client: dynamoClient,
  logStatements: true,
});

const testTable2 = new DynamoQuerier(simpleTableDefinition2, {
  tableName: TableName2,
  client: dynamoClient,
  logStatements: true,
});

const indexTable = new TableClient(indexTableDefinition, {
  tableName: TableName3,
  client: dynamoClient,
  logStatements: true,
});

const testTableClient = new TableClient(complexTableDefinitionQuery, {
  tableName: TableName,
  client: dynamoClient,
  logStatements: true,
});

const sortKeyAsIndexPartitionKeyTableClient = new TableClient(
  sortKeyAsIndexPartitionKeyTableDefinition,
  {
    tableName: SortKeyAsIndexPartitionKeyTableName,
    client: dynamoClient,
    logStatements: true,
  },
);

const identifier = 'query-items-test';
const indexIdentifier = 'query-items-index-test';

const preInserts: ComplexTable2[] = [
  {
    hash: 'query-items-test',
    text: 'some text',
    obj: { abc: 'xyz', def: 2 },
    mno: 2,
    pqr: 'yyy',
  },
  {
    hash: 'query-items-test-2',
    text: 'some other text',
    mno: 'abc',
    pqr: '123 456',
  },
  {
    hash: 'hash333',
    text: indexIdentifier,
    mno: 'abc',
    pqr: '123 456',
  },
  {
    hash: 'hash444',
    text: indexIdentifier,
    mno: 'abc',
    pqr: '123 456',
  },
  {
    hash: 'hash555',
    text: indexIdentifier,
    mno: 'abc',
    pqr: '123 456',
  },
];

const preInserts2: SimpleTable2[] = [
  { identifier, sort: '1', text: 'some text' },
  { identifier, sort: '2', text: 'some text 2' },
  { identifier, sort: '3', text: 'some text 3' },
  { identifier, sort: '4', text: 'some text 4' },
  { identifier: 'query-items-test2', sort: '55', text: 'some text 5' },
  { identifier: 'query-items-test2', sort: '56', text: 'some text 6' },
  { identifier: 'query-items-test2', sort: '67', text: 'some text 7' },
];

const preInserts3: IndexTable[] = [...new Array(10).keys()].map((index) => ({
  hash: `${index + 1}`,
  sort: `${index + 1}`,
  indexHash: `1`,
}));

describe('Dynamo Querier', () => {
  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
    await Promise.all(
      preInserts2.map((Item) =>
        dynamoClient.put({ TableName: TableName2, Item }),
      ),
    );
    await Promise.all(
      preInserts3.map((Item) =>
        dynamoClient.put({ TableName: TableName3, Item }),
      ),
    );
    await Promise.all(
      preInserts.map((Item) =>
        dynamoClient.put({
          TableName: SortKeyAsIndexPartitionKeyTableName,
          Item,
        }),
      ),
    );
  });

  describe('Key conditions', () => {
    it('should find single item by partition', async () => {
      const result = await testTable.query({ hash: 'query-items-test' });
      expect(result.member).toEqual([preInserts[0]]);
    });

    it('should find multiple items by partition', async () => {
      const result = await testTable2.query({ identifier });
      expect(result.member).toEqual(preInserts2.slice(0, 4));
    });

    it('should find items by equality', async () => {
      const result = await testTable2.query({
        identifier,
        sort: (sortKey) => sortKey.eq('4'),
      });
      expect(result.member).toEqual([preInserts2[3]]);
    });

    it('should find items greater than', async () => {
      const result = await testTable2.query({
        identifier,
        sort: (sortKey) => sortKey.gt('2'),
      });
      expect(result.member).toEqual([preInserts2[2], preInserts2[3]]);
    });

    it('should find items less than', async () => {
      const result = await testTable2.query({
        identifier,
        sort: (sortKey) => sortKey.lt('2'),
      });
      expect(result.member).toEqual([preInserts2[0]]);
    });

    it('should find items greater than or equal to', async () => {
      const result = await testTable2.query({
        identifier,
        sort: (sortKey) => sortKey.gte('2'),
      });
      expect(result.member).toEqual([
        preInserts2[1],
        preInserts2[2],
        preInserts2[3],
      ]);
    });

    it('should find items less than or equal to', async () => {
      const result = await testTable2.query({
        identifier,
        sort: (sortKey) => sortKey.lte('2'),
      });
      expect(result.member).toEqual([preInserts2[0], preInserts2[1]]);
    });

    it('should find items between', async () => {
      const result = await testTable2.query({
        identifier,
        sort: (sortKey) => sortKey.between('2', '4'),
      });
      expect(result.member).toEqual([
        preInserts2[1],
        preInserts2[2],
        preInserts2[3],
      ]);
    });

    it('should find items that begins with', async () => {
      const result = await testTable2.query({
        identifier: 'query-items-test2',
        sort: (sortKey) => sortKey.beginsWith('5'),
      });
      expect(result.member).toEqual([preInserts2[4], preInserts2[5]]);
    });
  });

  describe('Limit / Next', () => {
    const firstIdLimit1Next =
      'eyJpZGVudGlmaWVyIjoicXVlcnktaXRlbXMtdGVzdCIsInNvcnQiOiIxIn0=';

    it('should limit to one item', async () => {
      const result = await testTable2.query({ identifier }, { limit: 1 });
      expect(result).toEqual({
        member: [preInserts2[0]],
        next: firstIdLimit1Next,
        count: 1,
        scannedCount: 1,
      });
    });

    it('should get next page when index', async () => {
      const result = await indexTable.index('index').queryAll(
        { indexHash: '1' },
        {
          limit: 1,
          next: 'eyJpbmRleEhhc2giOiIxIiwic29ydCI6IjMiLCJoYXNoIjoiMyJ9',
        },
      );
      expect(result.member).toEqual([{ indexHash: '1', hash: '4', sort: '4' }]);
      expect(result.next).toEqual(
        'eyJpbmRleEhhc2giOiIxIiwic29ydCI6IjQiLCJoYXNoIjoiNCJ9',
      );
      expect(result.count).toEqual(1);
    });

    it('should get rest of items after next token', async () => {
      const result = await testTable2.query(
        { identifier },
        { next: firstIdLimit1Next },
      );
      expect(result).toEqual({
        member: [preInserts2[1], preInserts2[2], preInserts2[3]],
        count: 3,
        scannedCount: 3,
      });
    });
  });

  describe('Filter', () => {
    async function filteredBy(
      filter: (
        text: Operation<SimpleTable2, string>,
      ) => CompareWrapperOperator<SimpleTable2>,
    ): Promise<QuerierReturn<SimpleTable2>> {
      return await testTable2.query(
        { identifier },
        { filter: (compare) => filter(compare().text) },
      );
    }

    it('should filter item matching on equality', async () => {
      const result = await filteredBy((text) => text.eq('some text 3'));
      expect(result).toEqual({
        member: [preInserts2[2]],
        count: 1,
        scannedCount: 4,
      });
    });

    it('should filter item matching greater than', async () => {
      const result = await filteredBy((text) => text.gt('some text 3'));
      expect(result).toEqual({
        member: [preInserts2[3]],
        count: 1,
        scannedCount: 4,
      });
    });

    it('should filter item matching less than', async () => {
      const result = await filteredBy((text) => text.lt('some text 3'));
      expect(result).toEqual({
        member: [preInserts2[0], preInserts2[1]],
        count: 2,
        scannedCount: 4,
      });
    });

    it('should filter item matching greater than or equal', async () => {
      const result = await filteredBy((text) => text.gte('some text 3'));
      expect(result).toEqual({
        member: [preInserts2[2], preInserts2[3]],
        count: 2,
        scannedCount: 4,
      });
    });

    it('should filter item matching less than or equal', async () => {
      const result = await filteredBy((text) => text.lte('some text 3'));
      expect(result).toEqual({
        member: [preInserts2[0], preInserts2[1], preInserts2[2]],
        count: 3,
        scannedCount: 4,
      });
    });

    it('should filter item matching not equal', async () => {
      const result = await filteredBy((text) => text.neq('some text 3'));
      expect(result).toEqual({
        member: [preInserts2[0], preInserts2[1], preInserts2[3]],
        count: 3,
        scannedCount: 4,
      });
    });

    it('should filter item matching between', async () => {
      const result = await filteredBy((text) =>
        text.between('some text 2', 'some text 4'),
      );
      expect(result).toEqual({
        member: [preInserts2[1], preInserts2[2], preInserts2[3]],
        count: 3,
        scannedCount: 4,
      });
    });

    it('should filter item matching in', async () => {
      const result = await filteredBy((text) =>
        text.in(['some text 2', 'some text 4']),
      );
      expect(result).toEqual({
        member: [preInserts2[1], preInserts2[3]],
        count: 2,
        scannedCount: 4,
      });
    });

    it('should filter item matching or', async () => {
      const result = await testTable2.query(
        { identifier },
        {
          filter: (compare) =>
            compare()
              .text.eq('some text 2')
              .or(compare().text.eq('some text 4')),
        },
      );
      expect(result).toEqual({
        member: [preInserts2[1], preInserts2[3]],
        count: 2,
        scannedCount: 4,
      });
    });

    it('should filter item matching and', async () => {
      const result = await testTable2.query(
        { identifier },
        {
          filter: (compare) =>
            compare()
              .text.gt('some text 2')
              .and(compare().text.gte('some text 4')),
        },
      );
      expect(result).toEqual({
        member: [preInserts2[3]],
        count: 1,
        scannedCount: 4,
      });
    });
  });

  describe('Scan Index Forward', () => {
    it('should reverse sort order', async () => {
      const result = await testTable2.query(
        { identifier },
        { scanIndexForward: false },
      );
      expect(result).toEqual({
        member: [
          preInserts2[3],
          preInserts2[2],
          preInserts2[1],
          preInserts2[0],
        ],
        count: 4,
        scannedCount: 4,
      });
    });
  });

  describe('Consumed Capacity', () => {
    it('should return consumed capacity', async () => {
      const result = await testTable2.query(
        { identifier },
        { returnConsumedCapacity: 'TOTAL' },
      );
      expect(result).toEqual({
        member: preInserts2.slice(0, 4),
        count: 4,
        scannedCount: 4,
        consumedCapacity: {
          CapacityUnits: 0.5,
          TableName: 'simpleTableDefinition2',
        },
      });
    });
  });

  describe('Query All', () => {
    it('should fetch all items', async () => {
      const result = await testTable2.queryAll({ identifier }, { limit: 5 });
      expect(result.member.length).toEqual(4);
    });
    it('should fetch exact number of items', async () => {
      const result = await testTable2.queryAll({ identifier }, { limit: 2 });
      expect(result.member.length).toEqual(2);
    });
    it('should fetch all from index items', async () => {
      const result = await testTableClient
        .index('abc')
        .queryAll({ text: indexIdentifier }, { limit: 100 });
      expect(result.member).toEqual(
        expect.arrayContaining(
          preInserts.filter((item) => item.text === indexIdentifier),
        ),
      );
    });
    it('should fetch exact number from index items', async () => {
      const result = await testTableClient
        .index('abc')
        .queryAll({ text: indexIdentifier }, { limit: 1 });
      expect(result.member.length).toEqual(1);
      expect(result.member[0].text).toEqual(indexIdentifier);
    });

    it('should fetch exact number from index items when partitionKey is same as sortKey in global index', async () => {
      const result = await sortKeyAsIndexPartitionKeyTableClient
        .index('abc')
        .queryAll({ text: indexIdentifier }, { limit: 1 });
      expect(result.member.length).toEqual(1);
      expect(result.member[0].text).toEqual(indexIdentifier);
    });
  });
});
