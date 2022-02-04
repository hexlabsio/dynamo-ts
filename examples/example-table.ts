import {defineTable} from "../src";
import {DynamoDB} from "aws-sdk";

export const exampleClient = new DynamoDB.DocumentClient({
    endpoint: 'localhost:8000',
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
            hashKey: 'make',
            rangeKey: 'model'
        }
    });