import { DynamoDefinition } from './types';

type Project<T extends DynamoDefinition, S extends string> =
  keyof T extends string
    ? S extends keyof T ?
    : never;