import {DynamoEntry} from "./dynamoTable";
import {CompareWrapperOperator, Operation, OperationType} from "./operation";
import {DynamoObjectDefinition, SimpleDynamoType} from "./type-mapping";

export type KeyComparisonBuilder<T> = {
    eq(value: T): void;
    lt(value: T): void;
    lte(value: T): void;
    gt(value: T): void;
    gte(value: T): void;
    between(a: T, b: T): void;
// eslint-disable-next-line @typescript-eslint/ban-types
} & (T extends string ? { beginsWith(value: string): void } : { });

export type ComparisonBuilder<T> = { [K in keyof T]: Operation<T, T[K]> } & {
    exists(path: string): CompareWrapperOperator<T>;
    notExists(path: string): CompareWrapperOperator<T>;
    isType(path: string, type: SimpleDynamoType): CompareWrapperOperator<T>;
    beginsWith(path: string, beginsWith: string): CompareWrapperOperator<T>;
    contains(path: string, operand: string): CompareWrapperOperator<T>;
    not(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
};

export class Wrapper {
    constructor(
        public names: Record<string, string> = {},
        public valueMappings: Record<string, unknown> = {},
        public expression: string = '',
    ) {}

    add(
        names: Record<string, string> = {},
        valueMappings: Record<string, unknown> = {},
        expression = '',
    ): Wrapper {
        this.names = { ...this.names, ...names };
        this.valueMappings = { ...this.valueMappings, ...valueMappings };
        this.expression = expression;
        return this;
    }

    and(comparison: Wrapper): Wrapper {
        this.add(
            comparison.names,
            comparison.valueMappings,
            `(${this.expression}) AND (${comparison.expression})`,
        );
        return this;
    }

    or(comparison: Wrapper): Wrapper {
        this.add(
            comparison.names,
            comparison.valueMappings,
            `(${this.expression}) OR (${comparison.expression})`,
        );
        return this;
    }

}

export class ComparisonBuilderType<
    D extends DynamoObjectDefinition['object'],
    T extends DynamoEntry<D>,
    > {
    public wrapper = new Wrapper();
    constructor(definition: D) {
        Object.keys(definition).forEach((key) => {
            (this as any)[key] = new OperationType(this.wrapper, key).operation();
        });
    }

    exists(path: string): Wrapper {
        return this.wrapper.add({}, {}, `attribute_exists(${path})`);
    }
    notExists(path: string): Wrapper {
        return this.wrapper.add({}, {}, `attribute_not_exists(${path})`);
    }
    private typeFor(type: SimpleDynamoType): string {
        const withoutOptional = type.endsWith('?') ? type.substring(0, type.length - 2) : type;
        switch (withoutOptional) {
            case 'string':
                return 'S';
            case 'string set':
                return 'SS';
            case 'number':
                return 'N';
            case 'number set':
                return 'NS';
            case 'binary':
                return 'B';
            case 'binary set':
                return 'BS';
            case 'boolean':
                return 'BOOL';
            case 'null':
                return 'NULL';
            case 'list':
                return 'L';
            default:
                return 'M';
        }
    }

    isType(path: string, type: SimpleDynamoType): Wrapper {
        const key = Math.floor(Math.random() * 10000000);
        return this.wrapper.add(
            {},
            { [`:${key}`]: this.typeFor(type) },
            `attribute_type(${path}, :${key})`,
        );
    }

    beginsWith(path: string, beginsWith: string): Wrapper {
        const key = Math.floor(Math.random() * 10000000);
        return this.wrapper.add(
            {},
            { [`:${key}`]: beginsWith },
            `begins_with(${path}, :${key})`,
        );
    }

    contains(key: keyof T, operand: string): Wrapper {
        const mapKey = Math.floor(Math.random() * 10000000);
        return this.wrapper.add(
            { [`#${mapKey}`]: key as string },
            { [`:${mapKey}`]: operand },
            `contains(#${mapKey}, :${mapKey})`,
        );
    }

    containsPath(path: string, operand: string): Wrapper {
        const key = Math.floor(Math.random() * 10000000);
        return this.wrapper.add(
            {},
            { [`:${key}`]: operand },
            `contains(${path}, :${key})`,
        );
    }

    not(comparison: Wrapper): Wrapper {
        this.wrapper.names = comparison.names;
        this.wrapper.valueMappings = comparison.valueMappings;
        this.wrapper.expression = `NOT (${comparison.expression})`;
        return this.wrapper;
    }

    builder(): ComparisonBuilder<T> {
        return this as unknown as ComparisonBuilder<T>;
    }
}