export interface KeySchema {
  KeyType: string;
  AttributeName: string;
}
export interface GlobalSecondaryIndexProperties {
  IndexName: string;
  ContributorInsightsSpecification?: {
    Enabled: boolean;
  };
  Projection: {
    NonKeyAttributes?: string[];
    ProjectionType?: string;
  };
  ProvisionedThroughput?: {
    WriteCapacityUnits: number;
    ReadCapacityUnits: number;
  };
  KeySchema: KeySchema[];
}

export interface LocalSecondaryIndexProperties {
  IndexName: string;
  Projection: {
    NonKeyAttributes?: string[];
    ProjectionType?: string;
  };
  KeySchema: KeySchema[];
}

export interface TableProperties {
  SSESpecification?: {
    SSEEnabled: boolean;
    SSEType?: string;
    KMSMasterKeyId?: string;
  };
  KinesisStreamSpecification?: {
    StreamArn: string;
  };
  StreamSpecification?: {
    StreamViewType: string;
  };
  ContributorInsightsSpecification?: {
    Enabled: boolean;
  };
  ImportSourceSpecification?: {
    S3BucketSource: {
      S3Bucket: string;
      S3KeyPrefix?: string;
      S3BucketOwner?: string;
    };
    InputFormat: string;
    InputFormatOptions?: {
      Csv?: {
        Delimiter?: string;
        HeaderList?: string[];
      };
    };
    InputCompressionType?: string;
  };
  PointInTimeRecoverySpecification?: {
    PointInTimeRecoveryEnabled?: boolean;
  };
  ProvisionedThroughput?: {
    WriteCapacityUnits: number;
    ReadCapacityUnits: number;
  };
  TableName?: string;
  AttributeDefinitions?: {
    AttributeType: string;
    AttributeName: string;
  }[];
  BillingMode?: string;
  GlobalSecondaryIndexes?: GlobalSecondaryIndexProperties[];
  KeySchema: KeySchema[];
  LocalSecondaryIndexes?: LocalSecondaryIndexProperties[];
  DeletionProtectionEnabled?: boolean;
  TableClass?: string;
  Tags?: any[];
  TimeToLiveSpecification?: {
    Enabled: boolean;
    AttributeName?: string;
  };
}
