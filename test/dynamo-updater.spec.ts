import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoUpdater } from '../src/dynamo-updater';
import { ComplexTable2, complexTableDefinitionQuery } from './tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

const TableName = 'complexTableDefinitionQuery';

const testTable = new DynamoUpdater<typeof complexTableDefinitionQuery>({
  tableName: TableName,
  client: dynamoClient,
  logStatements: true,
});

const hash = 'update-items-test';

const preInserts: ComplexTable2[] = [
  { hash, text: 'some text', obj: { abc: 'xyz', def: 2 }, mno: 2, pqr: 'yyy' },
  {
    hash: hash + '2',
    text: 'some text',
    obj: { abc: 'xyz', def: 2 },
    mno: 2,
    pqr: 'yyy',
  },
  {
    hash: hash + '3',
    text: 'some text',
    obj: { abc: 'xyz', def: 2 },
    mno: 2,
    pqr: 'yyy',
  },
  { hash: hash + '4', text: 'some other text', mno: 2, pqr: '123 456' },
];

describe('Dynamo Updater', () => {
  beforeAll(async () => {
    await Promise.all(
      preInserts.map((Item) => dynamoClient.put({ TableName, Item })),
    );
  });

  describe('Key conditions', () => {
    it('should update text field only', async () => {
      const result = await testTable.update({
        key: { hash },
        updates: { text: 'test' },
        return: 'ALL_NEW',
      });
      expect(result.item).toEqual({ ...preInserts[0], text: 'test' });
    });

    it('should act like a put when item does not exist', async () => {
      const result = await testTable.update({
        key: { hash: 'another-hash' },
        updates: { text: 'test', mno: 5 },
        return: 'ALL_NEW',
      });
      expect(result.item).toEqual({
        hash: 'another-hash',
        text: 'test',
        mno: 5,
      });
    });

    it('should remove value when undefined', async () => {
      const result = await testTable.update({
        key: { hash: hash + '2' },
        updates: { pqr: undefined },
        return: 'ALL_NEW',
      });
      expect(result.item).toEqual({ ...preInserts[1], pqr: undefined });
    });

    it('should update nested value', async () => {
      const result = await testTable.update({
        key: { hash: hash + '3' },
        updates: { 'obj.abc': 'zyz' },
        return: 'ALL_NEW',
      });
      expect(result.item).toEqual({
        ...preInserts[2],
        obj: { ...preInserts[2].obj, abc: 'zyz' },
      });
    });

    it('should increment value', async () => {
      const result = await testTable.update({
        key: { hash: hash + '4' },
        updates: { mno: 3 },
        increments: [{ key: 'mno' }],
        return: 'ALL_NEW',
      });
      expect(result.item).toEqual({ ...preInserts[3], mno: 5 });
    });

    it('should pass condition check hash check', async () => {
      const result = await testTable.update({
        key: { hash: hash + '4' },
        updates: { mno: 3 },
        return: 'ALL_NEW',
        condition: (compare) => compare().hash.exists,
      });
      expect(result.item).toEqual({ ...preInserts[3], mno: 3 });
    });

    it('should fail condition check', async () => {
      await expect(
        testTable.update({
          key: { hash: hash + '4' },
          updates: { mno: 3 },
          increments: [{ key: 'mno' }],
          condition: (compare) => compare().mno.isType('string'),
          return: 'ALL_NEW',
        }),
      ).rejects.toThrow('The conditional request failed');
    });
  });
});
