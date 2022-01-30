import * as crypto from "crypto";
import {PutItemInput} from "aws-sdk/clients/dynamodb";

export class AttributeNamer {
    static nameFor(name: string): string {
        return crypto.createHash('md5').update(name).digest('hex');
    }
    static uniqueName(): string {
        return `${Math.floor(Math.random() * 10000000)}`
    }
}

export class AttributeBuilder {

    private constructor(
        private readonly names: Record<string, string> = {},
        private values: Record<string, unknown> = {}
    ) {}

    static create(): AttributeBuilder {
        return new AttributeBuilder();
    }

    combine(builder: AttributeBuilder) {
        return new AttributeBuilder({...this.names, ...builder.names}, {...this.values, ...builder.values});
    }

    addNames(...names: string[]): AttributeBuilder {
        return new AttributeBuilder({...this.names, ...names.reduce((prev, name) => ({...prev, [name]: AttributeNamer.nameFor(name)}) , {}) }, this.values);
    }

    addValue(value: unknown): [string, AttributeBuilder] {
        const name = AttributeNamer.uniqueName();
        return [`:${name}`, new AttributeBuilder(this.names,{...this.values, [name]: value})];
    }

    nameFor(name: string): string {
        return `#${this.names[name]}`;
    }

    asInput(provided?: Pick<PutItemInput, 'ExpressionAttributeNames' | 'ExpressionAttributeValues'>): Pick<PutItemInput, 'ExpressionAttributeNames' | 'ExpressionAttributeValues'> {
        const keys = Object.keys(this.names);
        const providedNames = Object.keys(provided?.ExpressionAttributeNames ?? {});
        const providedValues = Object.keys(provided?.ExpressionAttributeNames ?? {});
        const valueKeys = Object.keys(this.values);
        const names = keys.length > 0 || providedNames.length > 0
            ? { ExpressionAttributeNames: {...(provided?.ExpressionAttributeNames ?? {}), ...keys.reduce((prev, key) => ({...prev, [this.nameFor(key)]: key}), {})}}
            : {};
        const values = valueKeys.length > 0 || providedValues.length > 0
            ? { ExpressionAttributeValues: {...(provided?.ExpressionAttributeNames ?? {}), ...valueKeys.reduce((prev, key) => ({...prev, [`:${key}`]: this.values[key]}), {})}}
            : {}
        return {...names, ...values};
    }
}