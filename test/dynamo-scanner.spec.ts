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

describe('Dynamo Scanner', () => {
  beforeAll(async () => {
    await testTable.put({
      hash: 'scan-item-test',
      text: 'some text',
      obj: { abc: 'def', def: 2 },
    });
    await testTable.put({
      hash: 'scan-item-test2',
      text: 'some text',
      arr: [],
    });
    await testTable.put({
      hash: 'scan-item-test3',
      text: 'some text',
      arr: [{ ghi: 'a' }, { ghi: 'b' }],
    });
    await testTable.put({
      hash: 'scan-item-test4',
      text: 'some text',
      arr: [{ ghi: 'a' }],
    });
    await testTable.put({ hash: 'scan-item-test5' });
  });

  it('should scan all items', async () => {
    const result = await testTable.scan();
    expect(result.member).toEqual([
      {
        obj: { def: 2, abc: 'def' },
        hash: 'scan-item-test',
        text: 'some text',
      },
      { hash: 'scan-item-test5' },
      {
        arr: [{ ghi: 'a' }, { ghi: 'b' }],
        hash: 'scan-item-test3',
        text: 'some text',
      },
      { arr: [], hash: 'scan-item-test2', text: 'some text' },
      { arr: [{ ghi: 'a' }], hash: 'scan-item-test4', text: 'some text' },
    ]);
  });

  it('should project items when scanning', async () => {
    const result = await testTable.scan({
      projection: (projector) =>
        projector.project('text').project('obj.def').project('arr.[1]'),
    });
    console.log(result.member);
    expect(result.member).toEqual([
      {
        obj: { def: 2 },
        text: 'some text',
      },
      {},
      {
        arr: [{ ghi: 'b' }],
        text: 'some text',
      },
      { text: 'some text' },
      { text: 'some text' },
    ]);
  });

  it('should scan and filter items that have a single entry in arr', async () => {
    const result = await testTable.scan({
      filter: (compare) => compare().existsPath('arr[1]'),
    });
    expect(result.member).toEqual([
      {
        hash: 'scan-item-test3',
        text: 'some text',
        arr: [{ ghi: 'a' }, { ghi: 'b' }],
      },
    ]);
  });
});
