import {
    DynamoEntry,
    DynamoMapDefinition,
} from "./type-mapping";
import {AttributeBuilder} from "./attribute-builder";

type PathKeys<T> = T extends (infer X)[]
    ? `[${number}]` | `[${number}].${PathKeys<X>}`
    : T extends object
        ? (keyof T extends string ? (keyof T | SubKeys<T, keyof T>) : never)
        : never;

type SubKeys<T, K extends keyof T> = K extends string ? `${K}.${PathKeys<T[K]>}` : never;

type TupleKeys<P extends string> = P extends `${infer A}.${infer TAIL}`
    ? [A , ...TupleKeys<TAIL>]
    : P extends `${infer A}`
        ? [A]
        : never

type ExtractPath<P extends string, T> =
        P extends `${infer A}.${infer TAIL}`
            ? (T extends any[] ? ExtractPath<TAIL, T[number]> : (A extends keyof T ? ExtractPath<TAIL, T[A]> : any))
            : P extends `${infer A}`
        ? (T extends any[] ? T[number] : (A extends keyof T ? T[A] : any))
        : never

type FromKeys<KEYS extends any[], V> =
    KEYS extends [infer K, ...infer KS]
        ? K extends `[${number}]` ? (FromKeys<KS, V>[] | undefined) : {[KEY in K extends string ? K : never]: FromKeys<KS, V>}
        : KEYS extends [infer K3] ? {[KEY in K3 extends string ? K3 : never]: V} : V;

interface Projector<DEFINITION extends DynamoMapDefinition, PROJECTED = {}> {
    project<PATH extends PathKeys<DynamoEntry<DEFINITION>>>(path: PATH): Projector<DEFINITION, PROJECTED & FromKeys<TupleKeys<PATH>, ExtractPath<PATH, DynamoEntry<DEFINITION>>>>
}

class ProjectorType<DEFINITION extends DynamoMapDefinition, PROJECTED = {}> implements Projector<DEFINITION, PROJECTED>{

    constructor(
        private readonly attributeBuilder: AttributeBuilder,
        readonly expression: string = ''
    ) {}

    project<PATH extends PathKeys<DynamoEntry<DEFINITION>>>(path: PATH): Projector<DEFINITION, PROJECTED & FromKeys<TupleKeys<PATH>, ExtractPath<PATH, DynamoEntry<DEFINITION>>>> {
        const projectionExpression = this.attributeBuilder.buildPath(path);
        return new ProjectorType(this.attributeBuilder, this.expression ? `${this.expression},${projectionExpression}` : projectionExpression)
    }
}

export type Projection<DEFINITION extends DynamoMapDefinition, R> = (projector: Projector<DEFINITION>) => Projector<DEFINITION, R>

export class ProjectionHandler {
    static projectionFor<DEFINITION extends DynamoMapDefinition>(attributeBuilder: AttributeBuilder, definition: DEFINITION, projection?: Projection<DEFINITION, any>): string {
        if(projection) {
            return (projection(new ProjectorType(attributeBuilder)) as ProjectorType<any, any>).expression;
        } else {
            const keys = Object.keys(definition);
            const updatedAttributes = attributeBuilder.addNames(...keys);
            return keys.map(key => updatedAttributes.nameFor(key)).join(',');
        }
    }
}