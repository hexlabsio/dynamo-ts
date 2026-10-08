/**
 * Output shapes for describing a table in infrastructure-as-code tools.
 *
 * These are plain objects so that dynamo-ts does not depend on any of the tools.
 */

export type TerraformKeySchema = {
  attribute_name: string;
  key_type: 'HASH' | 'RANGE';
};

/**
 * Arguments for a Terraform `aws_dynamodb_table` resource, using the snake_case names from the AWS provider.
 */
export type TerraformDynamoTable = {
  name: string;
  hash_key: string;
  range_key?: string;
  attribute: { name: string; type: 'S' | 'N' | 'B' }[];
  global_secondary_index?: {
    name: string;
    key_schema: TerraformKeySchema[];
    projection_type: 'ALL';
    read_capacity?: number;
    write_capacity?: number;
  }[];
  local_secondary_index?: {
    name: string;
    range_key: string;
    projection_type: 'ALL';
  }[];
};

/**
 * Extra Terraform arguments to pass through. The generated arguments can't be overridden.
 */
export type TerraformExtraArguments<Props> = Props & {
  [K in keyof TerraformDynamoTable]?: never;
} & { read_capacity?: number; write_capacity?: number };

export type CdkAttribute<A> = { name: string; type: A };

/**
 * Props for the CDK `aws_dynamodb.TableV2` construct.
 *
 * A and P are the CDK's AttributeType and ProjectionType enums.
 */
export type CdkTableProps<A, P> = {
  tableName?: string;
  partitionKey: CdkAttribute<A>;
  sortKey?: CdkAttribute<A>;
  globalSecondaryIndexes?: {
    indexName: string;
    partitionKey: CdkAttribute<A>;
    sortKey?: CdkAttribute<A>;
    projectionType: P;
  }[];
  localSecondaryIndexes?: {
    indexName: string;
    sortKey: CdkAttribute<A>;
    projectionType: P;
  }[];
};

/**
 * The parts of the `aws-cdk-lib/aws-dynamodb` module needed to build table props.
 */
export type CdkDynamoModule<A, P> = {
  AttributeType: { STRING: A; NUMBER: A; BINARY: A };
  ProjectionType: { ALL: P };
};

/**
 * Args for the SST v3 `sst.aws.Dynamo` component.
 */
export type SstDynamoArgs = {
  fields: Record<string, 'string' | 'number' | 'binary'>;
  primaryIndex: { hashKey: string; rangeKey?: string };
  globalIndexes?: Record<
    string,
    { hashKey: string; rangeKey?: string; projection: 'all' }
  >;
  localIndexes?: Record<string, { rangeKey: string; projection: 'all' }>;
};

// Map arguments are written as `key = { ... }`; every other object becomes a nested block
const HCL_MAP_ARGUMENTS = new Set(['tags', 'tags_all']);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hclString(value: string): string {
  // Escape template sequences so values are taken literally
  return JSON.stringify(value)
    .replace(/\$\{/g, () => '$${')
    .replace(/%\{/g, () => '%%{');
}

function hclKey(key: string): string {
  return /^[A-Za-z_][A-Za-z0-9_-]*$/.test(key) ? key : hclString(key);
}

function hclValue(value: unknown, indent: string): string {
  if (typeof value === 'string') return hclString(value);
  if (typeof value === 'number' || typeof value === 'boolean')
    return String(value);
  if (value === null) return 'null';
  if (Array.isArray(value))
    return `[${value.map((it) => hclValue(it, indent)).join(', ')}]`;
  if (isPlainObject(value)) {
    const inner = `${indent}  `;
    const entries = Object.entries(value)
      .filter(([, it]) => it !== undefined)
      .map(([key, it]) => `${inner}${hclKey(key)} = ${hclValue(it, inner)}`);
    return entries.length ? `{\n${entries.join('\n')}\n${indent}}` : '{}';
  }
  throw new Error(`Cannot write ${typeof value} as HCL`);
}

function hclBody(body: Record<string, unknown>, indent: string): string[] {
  return Object.entries(body).flatMap(([key, value]) => {
    if (value === undefined) return [];
    const blocks = Array.isArray(value) && value.every(isPlainObject);
    if (
      (blocks && value.length) ||
      (isPlainObject(value) && !HCL_MAP_ARGUMENTS.has(key))
    ) {
      return (blocks ? value : [value]).flatMap((block) => [
        `${indent}${key} {`,
        ...hclBody(block as Record<string, unknown>, `${indent}  `),
        `${indent}}`,
      ]);
    }
    return [`${indent}${key} = ${hclValue(value, indent)}`];
  });
}

/**
 * Writes a Terraform resource in HCL. Arrays of objects become repeated nested blocks, other objects become a single
 * nested block (apart from tags, which are maps).
 */
export function toHcl(
  type: string,
  name: string,
  body: Record<string, unknown>,
): string {
  return [
    `resource ${hclString(type)} ${hclString(name)} {`,
    ...hclBody(body, '  '),
    '}',
    '',
  ].join('\n');
}
