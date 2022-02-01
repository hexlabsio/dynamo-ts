module.exports = {
  "tables": [
    {
      "TableName": "simpleTableDefinition",
      "KeySchema": [
        {
          "KeyType": "HASH",
          "AttributeName": "identifier"
        }
      ],
      "AttributeDefinitions": [
        {
          "AttributeName": "identifier",
          "AttributeType": "S"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      }
    },
    {
      "TableName": "complexTableDefinition",
      "KeySchema": [
        {
          "KeyType": "HASH",
          "AttributeName": "hash"
        }
      ],
      "AttributeDefinitions": [
        {
          "AttributeName": "hash",
          "AttributeType": "S"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      }
    }
  ]
};