import {defineTable} from "../src/types";
import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/'  },
  region: 'local-env',
});
export const exampleClient = DynamoDBDocument.from(dynamo);

export const exampleCarTable = defineTable({
    make: 'string',
    identifier: 'string',
    model: 'string',
    year: 'number',
    colour: 'string'
},
    'make',
    'identifier',
    {
        'model-index': {
            partitionKey: 'make',
            sortKey: 'model'
        },
        'model-year-index': {
            partitionKey: 'model',
            sortKey: 'year'
        }
    });