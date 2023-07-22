module.exports = {
  "port": 5001,
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
      "TableName": "simpleTableDefinition2",
      "KeySchema": [
        {
          "KeyType": "HASH",
          "AttributeName": "identifier"
        },
        {
          "KeyType": "RANGE",
          "AttributeName": "sort"
        }
      ],
      "AttributeDefinitions": [
        {
          "AttributeName": "identifier",
          "AttributeType": "S"
        },
        {
          "AttributeName": "sort",
          "AttributeType": "S"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      }
    },
    {
      "TableName": "simpleTableDefinition3",
      "KeySchema": [
        {
          "KeyType": "HASH",
          "AttributeName": "identifier"
        },
        {
          "KeyType": "RANGE",
          "AttributeName": "sort"
        }
      ],
      "AttributeDefinitions": [
        {
          "AttributeName": "identifier",
          "AttributeType": "S"
        },
        {
          "AttributeName": "sort",
          "AttributeType": "S"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      }
    },
    {
      "TableName": "complexTableDefinitionQuery",
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
        },
        {
          "AttributeName": "text",
          "AttributeType": "S"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      },
      "GlobalSecondaryIndexes": [
        {
          "IndexName": "abc",
          "KeySchema": [
            {
              "KeyType": "HASH",
              "AttributeName": "text"
            }
          ],
          "ProvisionedThroughput": {
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
          },
          "Projection": {
            "ProjectionType": "ALL"
          }
        }
      ]
    },
    {
      "TableName": "setsTableDefinition",
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
      "TableName": "binaryTableDefinition",
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
      "TableName": "complexTableDefinitionFilter",
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
    },
    {
      "TableName": "deleteTableDefinition",
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
    },
    {
      "TableName": "indexTableDefinition",
      "KeySchema": [
        {
          "KeyType": "HASH",
          "AttributeName": "hash"
        },
        {
          "KeyType": "RANGE",
          "AttributeName": "sort"
        }
      ],
      "AttributeDefinitions": [
        {
          "AttributeName": "hash",
          "AttributeType": "S"
        },
        {
          "AttributeName": "sort",
          "AttributeType": "S"
        },
        {
          "AttributeName": "indexHash",
          "AttributeType": "S"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      },
      "GlobalSecondaryIndexes": [
        {
          "IndexName": "index",
          "KeySchema": [
            {
              "KeyType": "HASH",
              "AttributeName": "indexHash"
            },
            {
              "KeyType": "RANGE",
              "AttributeName": "sort"
            }
          ],
          "ProvisionedThroughput": {
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
          },
          "Projection": {
            "ProjectionType": "ALL"
          }
        }
      ]
    }
  ]
};