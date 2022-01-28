import {DynamoAnyEntry} from "./dynamoTable";

export type DynamoType = SimpleDynamoType | DynamoEntryDefinition;
export type DynamoObjectDefinition = {optional?: boolean, object: { [key: string]: DynamoType } };
export type DynamoArrayDefinition = {optional?: boolean, array: DynamoType };
export type DynamoEntryDefinition = DynamoObjectDefinition | DynamoArrayDefinition;

export type TypeFor<T extends DynamoType> = T extends 'string'
    ? string
    : T extends 'string set'
        ? string[]
        : T extends 'number'
            ? number
            : T extends 'number set'
                ? number[]
                : T extends 'binary'
                    ? Buffer
                    : T extends 'binary set'
                        ? Buffer[]
                        : T extends 'list'
                            ? unknown[]
                            : T extends 'map'
                                ? Record<string, unknown>
                                : T extends 'boolean'
                                    ? boolean
                                    : T extends 'null'
                                        ? null :
                                        T extends 'string?'
                                            ? string | undefined
                                            : T extends 'string set?'
                                                ? string[] | undefined
                                                : T extends 'number?'
                                                    ? number | undefined
                                                    : T extends 'number set?'
                                                        ? number[] | undefined
                                                        : T extends 'binary?'
                                                            ? Buffer | undefined
                                                            : T extends 'binary set?'
                                                                ? Buffer[] | undefined
                                                                : T extends 'list?'
                                                                    ? unknown[] | undefined
                                                                    : T extends 'map?'
                                                                        ? Record<string, unknown>
                                                                        : T extends 'boolean'
                                                                            ? boolean
                                                                            : T extends DynamoEntryDefinition
                                                                                ? (T['optional'] extends true ? (T extends {object: any} ? { [K in keyof T['object']]: DynamoAnyEntry<T['object']>[K] } : (T extends {array: any} ? DynamoAnyEntry<T['array']> : never)) | undefined : (T extends {object: any} ? { [K in keyof T['object']]: DynamoAnyEntry<T['object']>[K] } : (T extends {array: any} ? DynamoAnyEntry<T['array']> : never)))
                                                                                : never;


export type SimpleDynamoType =
    | 'string'
    | 'string set'
    | 'number'
    | 'number set'
    | 'binary set'
    | 'binary'
    | 'boolean'
    | 'null'
    | 'list'
    | 'map'
    | 'string?'
    | 'string set?'
    | 'number?'
    | 'number set?'
    | 'binary set?'
    | 'binary?'
    | 'boolean?'
    | 'list?'
    | 'map?';