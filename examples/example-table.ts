import {defineTable} from "../src/types";
import {DynamoDB} from "aws-sdk";

export const exampleClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:5001',
    sslEnabled: false,
    accessKeyId: 'xxxx',
    secretAccessKey: 'xxxx',
    region: 'local-env',
});

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