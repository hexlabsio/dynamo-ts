import {Wrapper} from "./comparison";
import {DynamoType, TypeFor} from "./type-mapping";
import {AttributeBuilder} from "./naming";

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
    public wrapper = new Wrapper(AttributeBuilder.create());
    constructor(private readonly key: string) {}

    private add(expression: (key: string, value: string) => string): (value: T) => Wrapper {
        return (value) => {
            const [valueKey, newBuilder] = this.wrapper.attributeBuilder.addNames(this.key).addValue(value);
            this.wrapper.attributeBuilder = newBuilder;
            const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
            return this.wrapper.add(expression(mappedKey, valueKey));
        };
    }

    eq = this.add((key, value) => `${key} = ${value}`);
    neq = this.add((key, value) => `${key} <> ${value}`);
    lt = this.add((key, value) => `${key} < ${value}`);
    lte = this.add((key, value) => `${key} <= ${value}`);
    gt = this.add((key, value) => `${key} > ${value}`);
    gte = this.add((key, value) => `${key} >= ${value}`);

    between(a: T, b: T): Wrapper {
        const builder = this.wrapper.attributeBuilder.addNames(this.key);
        const [aKey, builder2] = builder.addValue(a);
        const [bKey, builder3] = builder2.addValue(b);
        this.wrapper.attributeBuilder = builder3;
        const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
        return this.wrapper.add(`${mappedKey} BETWEEN ${aKey} AND :${bKey}`);
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

    private add(expression: (key: string, value: string) => string): (value: TypeFor<DynamoType>) => CompareWrapperOperator<any> {
        return (value) => {
            const [valueKey, newBuilder] = this.wrapper.attributeBuilder.addNames(this.key).addValue(value);
            this.wrapper.attributeBuilder = newBuilder;
            const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
            return this.wrapper.add(expression(mappedKey, valueKey));
        };
    }

    eq = this.add((key, value) => `${key} = ${value}`);
    neq = this.add((key, value) => `${key} <> ${value}`);
    lt = this.add((key, value) => `${key} < ${value}`);
    lte = this.add((key, value) => `${key} <= ${value}`);
    gt = this.add((key, value) => `${key} > ${value}`);
    gte = this.add((key, value) => `${key} >= ${value}`);

    between(a: TypeFor<DynamoType>, b: TypeFor<DynamoType>): CompareWrapperOperator<any> {
        const builder = this.wrapper.attributeBuilder.addNames(this.key);
        const [aKey, builder2] = builder.addValue(a);
        const [bKey, builder3] = builder2.addValue(b);
        this.wrapper.attributeBuilder = builder3;
        const mappedKey = this.wrapper.attributeBuilder.nameFor(this.key);
        return this.wrapper.add(`${mappedKey} BETWEEN ${aKey} AND :${bKey}`);
    }

    in(list: TypeFor<DynamoType>[]): CompareWrapperOperator<any> {
        const builder = this.wrapper.attributeBuilder.addNames(this.key);
        const mappedKey = builder.nameFor(this.key);
        const [valueKeys, newBuilder] = list.reduce<[string[], AttributeBuilder]>(
            ([keys, prevBuilder], it) => {
                const [key, updatedBuilder] = prevBuilder.addValue(it);
                return [[...keys, key], updatedBuilder]
            },
            [[], builder] as [string[], AttributeBuilder]
        );
        this.wrapper.attributeBuilder = newBuilder;
        return this.wrapper.add(`${mappedKey} IN (${valueKeys.join(',')})`,
        ) as any;
    }
}