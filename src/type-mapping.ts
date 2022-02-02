import {DynamoClientConfig} from "./dynamo-client-config";

export type DynamoIndexes<DEFINITION extends DynamoMapDefinition> = Record<string, { local?: boolean, hashKey: keyof DynamoEntry<DEFINITION>; rangeKey: keyof DynamoEntry<DEFINITION> | null }> | null

export type DynamoMapDefinition = { [key: string]: DynamoType };
export type DynamoType = SimpleDynamoType | DynamoEntryDefinition;
export type DynamoObjectDefinition = {optional?: boolean, object: DynamoMapDefinition };
export type DynamoArrayDefinition = {optional?: boolean, array: DynamoType };
export type DynamoEntryDefinition = DynamoObjectDefinition | DynamoArrayDefinition;


type UndefinedKeys<T> = { [P in keyof T]: undefined extends T[P] ? P : never}[keyof T];
type PartializeTop<T> = Partial<Pick<T, UndefinedKeys<T>>> & Omit<T, UndefinedKeys<T>>;
type PartializeObj<T> = {[K in keyof T]: T[K] extends Record<string, unknown> ? Partialize<T[K]>: T[K] extends (infer A)[] ? (A extends Record<string, unknown> ? Partialize<A> : A)[]: T[K]};
type Partialize<T> = PartializeObj<PartializeTop<T>>
export type DynamoEntry<T extends DynamoObjectDefinition['object']> = Partialize<{
    [K in keyof T]: TypeFor<T[K]>;
}>
export type IndexDefinition<T extends DynamoEntry<any>> = { hashKey: keyof T; rangeKey?: keyof T }

export type DynamoAnyEntry<T extends DynamoArrayDefinition['array'] | DynamoObjectDefinition['object']> = T extends DynamoObjectDefinition['object'] ? {
    [K in keyof T]: TypeFor<T[K]>;
} : T extends DynamoArrayDefinition['array'] ? TypeFor<T>[] : never;



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
                                                                        ? Record<string, unknown> | undefined
                                                                        : T extends 'boolean?'
                                                                            ? boolean | undefined
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

export type DynamoRangeKey<DEFINITION extends DynamoMapDefinition, HASH extends keyof DynamoClientConfig<DEFINITION>['tableType']> =
    Omit<keyof DynamoClientConfig<DEFINITION>['tableType'], HASH> | null

export type DynamoKeysFrom<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoClientConfig<DEFINITION>['tableType'],
    RANGE extends DynamoRangeKey<DEFINITION, HASH>
> = RANGE extends string
        ? RANGE extends keyof DynamoClientConfig<DEFINITION>['tableType']
            ? { [K in HASH | RANGE]: DynamoClientConfig<DEFINITION>['tableType'][K] }
                : never
        : { [K in HASH]: DynamoClientConfig<DEFINITION>['tableType'][K] }

export type DynamoNonKeysFrom<
    DEFINITION extends DynamoMapDefinition,
    HASH extends keyof DynamoClientConfig<DEFINITION>['tableType'],
    RANGE extends DynamoRangeKey<DEFINITION, HASH>
    > = RANGE extends string
    ? RANGE extends keyof DynamoClientConfig<DEFINITION>['tableType']
        ? Omit<DynamoClientConfig<DEFINITION>['tableType'], HASH | RANGE>
        : never
    : Omit<DynamoClientConfig<DEFINITION>['tableType'], HASH>