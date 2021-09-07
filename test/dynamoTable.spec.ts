import {DynamoDB} from "aws-sdk";
import {defineTable} from "../src";
import {DynamoTable} from "../src/dynamoTable";

export const testTable = defineTable({
  definition: { identifier: 'string', date: 'string', account: 'string', cost: 'number', usage: 'number', currency: 'string', lineItemType: 'string' },
  hashKey: 'identifier'
})

const updateMock = jest.fn().mockImplementation(() => ({promise: () => Promise.resolve({Attributes: {}})}));

const fakeDynamo: DynamoDB.DocumentClient = {
  update: updateMock
} as any;

describe('Dynamo Table', () => {
  it('should update multiple increments', async () => {
    const table = DynamoTable.build('testTable', fakeDynamo, testTable);
    await table.update({identifier: 'test'}, {account: 'another', cost: 5, usage: 6}, [{key: 'cost', start: 0}, {key: 'usage'}]);
    const cost = '4e1566f0798fb3d6f350720cacd74446';
    expect(updateMock).toBeCalledWith({
      ExpressionAttributeNames: {
        [`#${cost}`]: "cost",
        "#9366282e11c151558bdfaab4a264aa1b": "usage",
        "#e268443e43d93dab7ebef303bbe9642f": "account"
      },
      ExpressionAttributeValues: {
        [`:${cost}`]: 5,
        [`:${cost}start`]: 0,
        ":9366282e11c151558bdfaab4a264aa1b": 6,
        ":e268443e43d93dab7ebef303bbe9642f": "another",
      },
      Key: {
        identifier: "test"
      },
      TableName: "testTable",
      UpdateExpression: `SET #e268443e43d93dab7ebef303bbe9642f = :e268443e43d93dab7ebef303bbe9642f, #${cost} = if_not_exists(#${cost}, :${cost}start) + :4e1566f0798fb3d6f350720cacd74446, #9366282e11c151558bdfaab4a264aa1b = #9366282e11c151558bdfaab4a264aa1b + :9366282e11c151558bdfaab4a264aa1b`
    });
  });
});
