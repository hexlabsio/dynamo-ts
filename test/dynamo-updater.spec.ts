import { DynamoDB } from 'aws-sdk';
import { DynamoUpdater } from '../src/dynamo-updater';
import { DynamoTypeFrom } from '../src';
import { complexTableDefinitionQuery } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

type TableType = DynamoTypeFrom<typeof complexTableDefinitionQuery>;

const TableName = 'complexTableDefinitionQuery';

const testTable = new DynamoUpdater(complexTableDefinitionQuery, {
  tableName: TableName,
  client: dynamoClient,
  logStatements: true,
});

const hash = 'update-items-test';

const preInserts: TableType[] = [
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
      preInserts.map((Item) => dynamoClient.put({ TableName, Item }).promise()),
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

    it('should fail condition check', async () => {
      await expect(
        testTable.update({
          key: { hash: hash + '4' },
          updates: { mno: 3 },
          increments: [{ key: 'mno' }],
          condition: (compare) => compare().isType('mno', 'string'),
          return: 'ALL_NEW',
        }),
      ).rejects.toThrow('The conditional request failed');
    });
  });
});
