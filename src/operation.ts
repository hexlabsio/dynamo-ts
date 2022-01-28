import {Wrapper} from "./comparison";
import {DynamoType, TypeFor} from "./type-mapping";
import {nameFor} from "./naming";

export type CompareWrapperOperator<T> = {
    and(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
    or(comparison: CompareWrapperOperator<T>): CompareWrapperOperator<T>;
};

export type Operation<T, V> = {
    eq(value: V): CompareWrapperOperator<T>;
    neq(value: V): CompareWrapperOperator<T>;
    lt(value: V): CompareWrapperOperator<T>;
    lte(value: V): CompareWrapperOperator<T>;
    gt(value: V): CompareWrapperOperator<T>;
    gte(value: V): CompareWrapperOperator<T>;
    between(a: V, b: V): CompareWrapperOperator<T>;
    in(b: V[]): CompareWrapperOperator<T>;
};


export class KeyOperation<T> {
    public wrapper = new Wrapper();
    constructor(private readonly key: string) {}

    private add(expression: (key: string, value: string) => string): (value: T) => Wrapper {
        return (value) => {
            const mappedKey = nameFor(this.key);
            const mappedValue = Math.floor(Math.random() * 10000000).toString();
            return this.wrapper.add(
                { [`#${mappedKey}`]: this.key },
                { [`:${mappedValue}`]: value },
                expression(mappedKey, mappedValue),
            ) as any;
        };
    }

    eq = this.add((key, value) => `#${key} = :${value}`);
    neq = this.add((key, value) => `#${key} <> :${value}`);
    lt = this.add((key, value) => `#${key} < :${value}`);
    lte = this.add((key, value) => `#${key} <= :${value}`);
    gt = this.add((key, value) => `#${key} > :${value}`);
    gte = this.add((key, value) => `#${key} >= :${value}`);

    between(a: T, b: T): Wrapper {
        const mappedKey = nameFor(this.key);
        return this.wrapper.add(
            { [`#${mappedKey}`]: this.key },
            { [`:${mappedKey}1`]: a, [`:${mappedKey}2`]: b },
            `#${mappedKey} BETWEEN :${mappedKey}1 AND :${mappedKey}2`
        );
    }
}

export class OperationType {
    constructor(
        private readonly wrapper: Wrapper,
        private readonly key: string,
    ) {}
    operation(): Operation<any, any> {
        return this as unknown as Operation<any, any>;
    }

    private add(
        expression: (key: string, value: string) => string,
    ): (value: TypeFor<DynamoType>) => CompareWrapperOperator<any> {
        return (value) => {
            const mappedKey = nameFor(this.key);
            const mappedValue = Math.floor(Math.random() * 10000000).toString();
            return this.wrapper.add(
                { [`#${mappedKey}`]: this.key },
                { [`:${mappedValue}`]: value },
                expression(mappedKey, mappedValue),
            ) as any;
        };
    }

    eq = this.add((key, value) => `#${key} = :${value}`);
    neq = this.add((key, value) => `#${key} <> :${value}`);
    lt = this.add((key, value) => `#${key} < :${value}`);
    lte = this.add((key, value) => `#${key} <= :${value}`);
    gt = this.add((key, value) => `#${key} > :${value}`);
    gte = this.add((key, value) => `#${key} >= :${value}`);

    between(
        a: TypeFor<DynamoType>,
        b: TypeFor<DynamoType>,
    ): CompareWrapperOperator<any> {
        const mappedKey = nameFor(this.key);
        const aKey = `:${mappedKey}1`;
        const bKey = `:${mappedKey}2`;
        return this.wrapper.add(
            { [`#${mappedKey}`]: this.key },
            { [aKey]: a, [bKey]: b },
            `#${mappedKey} BETWEEN ${aKey} AND ${bKey}`,
        ) as any;
    }
    in(list: TypeFor<DynamoType>[]): CompareWrapperOperator<any> {
        const mappedKey = nameFor(this.key);
        const valueMappings = list.reduce(
            (agg, it, index) => ({ ...agg, [`:${mappedKey}${index}`]: it }),
            {} as any,
        );
        return this.wrapper.add(
            { [`#${mappedKey}`]: this.key },
            valueMappings,
            `#${mappedKey} IN (${Object.keys(valueMappings)
                .map((it) => `${it}`)
                .join(',')})`,
        ) as any;
    }
}