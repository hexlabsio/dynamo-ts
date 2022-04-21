import { DynamoDB } from "aws-sdk";
import { TableClient } from "../src";
import { complexTableDefinition } from "./tables";

const dynamoClient = new DynamoDB.DocumentClient({
  endpoint: "localhost:5001",
  sslEnabled: false,
  accessKeyId: "xxxx",
  secretAccessKey: "xxxx",
  region: "local-env",
});

const testTable = TableClient.build(complexTableDefinition, {
  tableName: "complexTableDefinition",
  client: dynamoClient,
  logStatements: true,
});

describe("Dynamo Updater", () => {
  beforeAll(async () => {
    await testTable.put({
      hash: "update-item-test1",
      text: "some text",
      obj: { abc: "def", def: 2 },
    });
    await testTable.put({
      hash: "update-item-test2",
      text: "some text",
      obj: { abc: "def", def: 2 },
    });
    await testTable.put({
      hash: "update-item-test3",
      text: "some text",
      obj: { abc: "def", def: 2 },
      jkl: 2,
    });
  });
  afterAll(async () => {
    await testTable.delete({ hash: "update-item-test1" });
    await testTable.delete({ hash: "update-item-test2" });
    await testTable.delete({ hash: "update-item-test3" });
  });

  it("should update item", async () => {
    const result = await testTable.update({
      key: { hash: "update-item-test1" },
      updates: { text: "updated", obj: undefined },
      return: "ALL_NEW",
    });
    expect(result.item).toEqual({
      hash: "update-item-test1",
      text: "updated",
    });
  });

  it("should use increment field if in item", async () => {
    const result = await testTable.update({
      key: { hash: "update-item-test2" },
      updates: { text: "updated", jkl: 2 },
      increments: [{ key: "jkl", start: 0 }],
      return: "ALL_NEW",
    });
    expect(result.item).toEqual({
      hash: "update-item-test2",
      obj: { abc: "def", def: 2 },
      text: "updated",
      jkl: 2,
    });
  });

  it("should use start if field not in item", async () => {
    await testTable.delete({
      hash: "update-item-test",
    });
    const result = await testTable.update({
      key: { hash: "update-item-test3" },
      updates: { text: "updated", jkl: 3 },
      increments: [{ key: "jkl", start: 0 }],
      return: "ALL_NEW",
    });
    expect(result.item).toEqual({
      hash: "update-item-test3",
      obj: { abc: "def", def: 2 },
      text: "updated",
      jkl: 5,
    });
  });
});
