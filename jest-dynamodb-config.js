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
      "TableName": "complexTableDefinitionScan",
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
      "TableName": "exampleCarTableDefinition",
      "KeySchema": [
        {
          "KeyType": "HASH",
          "AttributeName": "make"
        },
        {
          "KeyType": "RANGE",
          "AttributeName": "identifier"
        }
      ],
      "AttributeDefinitions": [
        {
          "AttributeName": "make",
          "AttributeType": "S"
        },
        {
          "AttributeName": "identifier",
          "AttributeType": "S"
        },
        {
          "AttributeName": "model",
          "AttributeType": "S"
        },
        {
          "AttributeName": "year",
          "AttributeType": "N"
        }
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
      },
      "GlobalSecondaryIndexes": [
        {
          "IndexName": "model-index",
          "KeySchema": [
            {
              "KeyType": "HASH",
              "AttributeName": "make"
            },
            {
              "KeyType": "RANGE",
              "AttributeName": "model"
            }
          ],
          "ProvisionedThroughput": {
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
          },
          "Projection": {
            "ProjectionType": "ALL"
          }
        },
        {
          "IndexName": "model-year-index",
          "KeySchema": [
            {
              "KeyType": "HASH",
              "AttributeName": "model"
            },
            {
              "KeyType": "RANGE",
              "AttributeName": "year"
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