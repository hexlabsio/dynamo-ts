import {AttributeBuilder} from './attribute-builder';
import { DynamoInfo, TypeFromDefinition } from './types';

type PathKeys<T> = T extends (infer X)[]
  ? `[${number}]` | `[${number}].${PathKeys<X>}`
  : T extends object
    ? keyof T extends string
      ? keyof T | SubKeys<T, keyof T>
      : never
    : never;

type SubKeys<T, K extends keyof T> = K extends string
  ? `${K}.${PathKeys<T[K]>}`
  : never;

type TupleKeys<P extends string> = P extends `${infer A}.${infer TAIL}`
  ? [A, ...TupleKeys<TAIL>]
  : P extends `${infer A}`
    ? [A]
    : never;

type ExtractPath<P extends string, T> = P extends `${infer A}.${infer TAIL}`
  ? T extends any[]
    ? ExtractPath<TAIL, T[number]>
    : A extends keyof T
      ? ExtractPath<TAIL, T[A]>
      : any
  : P extends `${infer A}`
    ? T extends any[]
      ? T[number]
      : A extends keyof T
        ? T[A]
        : any
    : never;

type FromKeys<KEYS extends any[], V> = KEYS extends [infer K, ...infer KS]
  ? K extends `[${number}]`
    ? FromKeys<KS, V>[] | undefined
    : { [KEY in K extends string ? K : never]: FromKeys<KS, V> }
  : KEYS extends [infer K3]
    ? { [KEY in K3 extends string ? K3 : never]: V }
    : V;

interface Projector<DEFINITION extends DynamoInfo, PROJECTED = {}> {
  project<PATH extends PathKeys<TypeFromDefinition<DEFINITION['definition']>>>(
    path: PATH,
  ): Projector<DEFINITION,
    PROJECTED &
    FromKeys<TupleKeys<PATH>, ExtractPath<PATH, TypeFromDefinition<DEFINITION['definition']>>>>;
}

export class ProjectorType<DEFINITION extends DynamoInfo,
  PROJECTED = {},
  > implements Projector<DEFINITION, PROJECTED> {
  constructor(
    private readonly attributeBuilder: AttributeBuilder,
    readonly expression: string = '',
    readonly projectionFields: (keyof DEFINITION)[] = [],
  ) {}

  project<PATH extends PathKeys<TypeFromDefinition<DEFINITION['definition']>>>(
    path: PATH,
  ): Projector<DEFINITION,
    PROJECTED &
    FromKeys<TupleKeys<PATH>, ExtractPath<PATH, TypeFromDefinition<DEFINITION['definition']>>>> {
    const projectionExpression = this.attributeBuilder.buildPath(path);
    return new ProjectorType(
      this.attributeBuilder,
      this.expression
        ? `${this.expression},${projectionExpression}`
        : projectionExpression,
      [path as any, ...this.projectionFields],
    );
  }
}

export type Projection<DEFINITION extends DynamoInfo, R> = (
  projector: Projector<DEFINITION>,
) => Projector<DEFINITION, R>;

export class ProjectionHandler {
  static projectionExpressionFor<DEFINITION extends DynamoInfo>(
    attributeBuilder: AttributeBuilder,
    definition: DEFINITION,
    projection?: Projection<DEFINITION, any>,
  ): string {
    if (projection) {
      return this.projectionFor(attributeBuilder, projection).expression;
    } else {
      return this.addDefinitionProjection(attributeBuilder, definition);
    }
  }

  static projectionWithKeysFor<DEFINITION extends DynamoInfo>(
    attributeBuilder: AttributeBuilder,
    definition: DEFINITION,
    hashKey: keyof TypeFromDefinition<DEFINITION['definition']>,
    rangeKey: keyof TypeFromDefinition<DEFINITION['definition']> | null,
    indexHashKey: keyof TypeFromDefinition<DEFINITION['definition']> | null,
    indexRangKey: keyof TypeFromDefinition<DEFINITION['definition']> | null,
    projection?: Projection<DEFINITION, any>,
  ): [string, string[]] {
    if (projection) {
      const projector = this.projectionFor(attributeBuilder, projection);
      const baseProjectionFields = projector.projectionFields;
      const enrichedFields = [rangeKey, indexHashKey, indexRangKey].reduce(
        (acc, elem) => (elem && !acc.includes(elem) && !baseProjectionFields.includes(elem as any)) ? [elem, ...acc] : acc,
        baseProjectionFields.includes(hashKey as any) ? [] : [hashKey],
      );
      const updatedProjector = enrichedFields.reduce(
        (p, ef) => p.project(ef as any) as any,
        projector,
      );
      return [updatedProjector.expression, enrichedFields as string[]];
    } else {
      return [this.addDefinitionProjection(attributeBuilder, definition), []];
    }
  }

  static projectionFor<DEFINITION extends DynamoInfo>(
    attributeBuilder: AttributeBuilder,
    projection: Projection<DEFINITION, any>,
  ): ProjectorType<DEFINITION, any> {
    return projection(new ProjectorType(attributeBuilder)) as ProjectorType<any,
      any>;
  }

  static addDefinitionProjection<DEFINITION extends DynamoInfo>(
    attributeBuilder: AttributeBuilder,
    definition: DEFINITION,
  ): string {
    const keys = Object.keys(definition.definition);
    const updatedAttributes = attributeBuilder.addNames(...keys);
    return keys.map((key) => updatedAttributes.nameFor(key)).join(',');
  }
}
