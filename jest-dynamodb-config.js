module.exports = {
    tables: [
        {
            TableName: 'test-get-table',
            KeySchema: [{AttributeName: 'identifier', KeyType: 'HASH'}],
            AttributeDefinitions: [{AttributeName: 'identifier', AttributeType: 'S'}],
            ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
        },
        {
            TableName: 'test-put-table',
            KeySchema: [{AttributeName: 'identifier', KeyType: 'HASH'}],
            AttributeDefinitions: [{AttributeName: 'identifier', AttributeType: 'S'}],
            ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
        },
    ],
};