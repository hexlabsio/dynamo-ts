import {DynamoArrayDefinition, DynamoObjectDefinition, DynamoType, SimpleDynamoType, TypeFor} from "./type-mapping";

type KeyString<T> = keyof T extends string ? keyof T: never;

type Keys<T> = T extends DynamoObjectDefinition ? KeyString<T['object']> : T extends DynamoArrayDefinition ? 'get' : KeyString<T>
type ValueFromKey<T, K extends Keys<T>> = T extends DynamoObjectDefinition ? T['object'][K] : T extends DynamoArrayDefinition ? T['array'] : K extends keyof T ? T[K] : never;

type UnknownTypeProjector = {
    get(key: string): CompleteType<any> & UnknownTypeProjector;
    getIndex(index: number): CompleteType<any> & UnknownTypeProjector;
}

type CompleteType<T extends DynamoType> = {
    type: TypeFor<T>;
    projection: () => string
};
type ChildProjector<T extends DynamoType, PARENT> = PARENT extends DynamoArrayDefinition
    ? (index: number) => CompleteType<T> & Projector<T>
    : T extends SimpleDynamoType
        ? T extends 'map' ? CompleteType<T> & UnknownTypeProjector
            : CompleteType<T> & Projector<T>
        : CompleteType<T> & Projector<T>

type Projector<T> = { [KEY in Keys<T>]: ValueFromKey<T, KEY> extends DynamoType ? ChildProjector<ValueFromKey<T, KEY>, T> : never }

// const aa: Projector<{object: {abc: 'string', def: {object: {ghi: 'number'}}, def2: 'map', hij: {array: {object: {xyz: 'string'}}}}}>;
