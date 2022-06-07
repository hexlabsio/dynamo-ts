import { DynamoDB } from 'aws-sdk';
import { TableClient } from '../src';
import { complexTableDefinition } from './tables';

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: 'localhost:5001',
  sslEnabled: false,
  accessKeyId: 'xxxx',
  secretAccessKey: 'xxxx',
  region: 'local-env',
});

const testTable = TableClient.build(complexTableDefinition, {
  tableName: 'complexTableDefinition',
  client: dynamoClient,
  logStatements: true,
});

describe('Dynamo Updater', () => {
  beforeAll(async () => {
    testTable.logStatements(false);
    await testTable.put({
      hash: 'update-item-test1',
      text: 'some text',
      obj: { abc: 'def', def: 2 },
    });
    await testTable.put({
      hash: 'update-item-test2',
      text: 'some text',
      obj: { abc: 'def', def: 2 },
    });
    await testTable.put({
      hash: 'update-item-test3',
      text: 'some text',
      obj: { abc: 'def', def: 2 },
      jkl: 2,
    });
    await testTable.put({
      hash: 'update-item-test4',
      text: 'some text',
      obj: { abc: 'def', def: 99 },
    });
    await testTable.put({
      hash: 'update-item-test5',
      text: 'some text',
      arr: [{ ghi: 0 }, { ghi: 0 }],
    });
    await testTable.put({
      hash: 'update-item-test6',
      text: 'some text',
      obj: { abc: 'def', def: 0 },
    });
    await testTable.put({
      hash: 'update-item-test7',
      text: 'some text',
      arr: [{ ghi: 0 }, { ghi: 0 }],
    });
    await testTable.put({
      hash: 'update-item-test8',
      text: 'some text',
      obj: { abc: 'zzz', def: 0 },
    });
    await testTable.put({
      hash: 'update-item-test9',
      text: 'some text',
      arr: [{ ghi: 0 }, { ghi: 1 }],
    });
    await testTable.put({
      hash: 'update-item-test10',
      text: 'some text',
      obj: { abc: 'def', def: undefined },
    });
    await testTable.put({
      hash: 'update-item-test11',
      text: 'some text',
      arr: [{ ghi: undefined }, { ghi: undefined }],
    });
    testTable.logStatements(true);
  });
  afterAll(async () => {
    testTable.logStatements(false);
    await Promise.all(
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((it) =>
        testTable.delete({ hash: `update-item-test${it}` }),
      ),
    );
    testTable.logStatements(true);
  });

  it('should update item', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test1' },
      updates: { text: 'updated', obj: undefined },
      return: 'ALL_NEW',
    });
    expect(result.item).toEqual({
      hash: 'update-item-test1',
      text: 'updated',
    });
  });

  it('should increment field if in item', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test2' },
      updates: { text: 'updated', jkl: 2 },
      increments: [{ key: 'jkl', start: 0 }],
      return: 'ALL_NEW',
    });
    expect(result.item).toEqual({
      hash: 'update-item-test2',
      obj: { abc: 'def', def: 2 },
      text: 'updated',
      jkl: 2,
    });
  });

  it('should use start if field not in item', async () => {
    await testTable.delete({
      hash: 'update-item-test',
    });
    const result = await testTable.update({
      key: { hash: 'update-item-test3' },
      updates: { text: 'updated', jkl: 3 },
      increments: [{ key: 'jkl', start: 0 }],
      return: 'ALL_NEW',
    });
    expect(result.item).toEqual({
      hash: 'update-item-test3',
      obj: { abc: 'def', def: 2 },
      text: 'updated',
      jkl: 5,
    });
  });

  it('should update nested object', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test4' },
      updates: { 'obj.def': 100 },
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      obj: { def: 100, abc: 'def' },
      hash: 'update-item-test4',
      text: 'some text',
    });
  });

  it('should update nested array', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test5' },
      updates: { 'arr.[0].ghi': 2, 'arr.[1].ghi': 3 },
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      arr: [{ ghi: 2 }, { ghi: 3 }],
      hash: 'update-item-test5',
      text: 'some text',
    });
  });

  it('should increment nested object if in item', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test6' },
      updates: { 'obj.abc': 'efg', 'obj.def': 1 },
      increments: [{ key: 'obj.def', start: 0 }],
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      obj: { def: 1, abc: 'efg' },
      hash: 'update-item-test6',
      text: 'some text',
    });
  });

  it('should use start value if not in item', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test10' },
      updates: { 'obj.abc': 'efg', 'obj.def': 1 },
      increments: [{ key: 'obj.def', start: 1 }],
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      obj: { def: 2, abc: 'efg' },
      hash: 'update-item-test10',
      text: 'some text',
    });
  });

  it('should increment nested array', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test7' },
      updates: { 'arr.[1000].ghi': 2, 'arr.[1].ghi': 3 },
      increments: [
        { key: 'arr.[0].ghi', start: 0 },
        { key: 'arr.[1].ghi', start: 1 },
      ],
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      arr: [{ ghi: 2 }, { ghi: 3 }],
      hash: 'update-item-test7',
      text: 'some text',
    });
  });

  it('should use start value if not in array', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test11' },
      updates: { 'arr.[0].ghi': 2, 'arr.[1].ghi': 3 },
      increments: [
        { key: 'arr.[0].ghi', start: 1 },
        { key: 'arr.[1].ghi', start: 1 },
      ],
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      arr: [{ ghi: 3 }, { ghi: 4 }],
      hash: 'update-item-test11',
      text: 'some text',
    });
  });

  it('remove nested object', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test8' },
      updates: { 'obj.def': undefined },
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      obj: { abc: 'zzz' },
      hash: 'update-item-test8',
      text: 'some text',
    });
  });

  it('remove nested array', async () => {
    const result = await testTable.update({
      key: { hash: 'update-item-test9' },
      updates: { 'arr.[0]': undefined },
      return: 'ALL_NEW',
    });

    expect(result.item).toEqual({
      arr: [{ ghi: 1 }],
      hash: 'update-item-test9',
      text: 'some text',
    });
  });
});
