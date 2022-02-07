import { DynamoEntry, DynamoMapDefinition } from './type-mapping';
import { AttributeBuilder } from './attribute-builder';

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

interface Projector<DEFINITION extends DynamoMapDefinition, PROJECTED = {}> {
  project<PATH extends PathKeys<DynamoEntry<DEFINITION>>>(
    path: PATH,
  ): Projector<
    DEFINITION,
    PROJECTED &
      FromKeys<TupleKeys<PATH>, ExtractPath<PATH, DynamoEntry<DEFINITION>>>
  >;
}

export class ProjectorType<
  DEFINITION extends DynamoMapDefinition,
  PROJECTED = {},
> implements Projector<DEFINITION, PROJECTED>
{
  constructor(
    private readonly attributeBuilder: AttributeBuilder,
    readonly expression: string = '',
    readonly projectionFields: (keyof DEFINITION)[] = [],
  ) {}

  project<PATH extends PathKeys<DynamoEntry<DEFINITION>>>(
    path: PATH,
  ): Projector<
    DEFINITION,
    PROJECTED &
      FromKeys<TupleKeys<PATH>, ExtractPath<PATH, DynamoEntry<DEFINITION>>>
  > {
    const projectionExpression = this.attributeBuilder.buildPath(path);
    return new ProjectorType(
      this.attributeBuilder,
      this.expression
        ? `${this.expression},${projectionExpression}`
        : projectionExpression,
      [path as string, ...this.projectionFields],
    );
  }
}

export type Projection<DEFINITION extends DynamoMapDefinition, R> = (
  projector: Projector<DEFINITION>,
) => Projector<DEFINITION, R>;

export class ProjectionHandler {
  static projectionExpressionFor<DEFINITION extends DynamoMapDefinition>(
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

  static projectionWithKeysFor<DEFINITION extends DynamoMapDefinition>(
    attributeBuilder: AttributeBuilder,
    definition: DEFINITION,
    hashKey: keyof DynamoEntry<DEFINITION>,
    rangeKey: keyof DynamoEntry<DEFINITION> | null,
    indexName: keyof DynamoEntry<DEFINITION> | null,
    projection?: Projection<DEFINITION, any>,
  ): [string, string[]] {
    if (projection) {
      const baseProjector = this.projectionFor(attributeBuilder, projection);
      const keyFields = [rangeKey, indexName].reduce(
        (acc, elem) => (elem ? [elem, ...acc] : acc),
        [hashKey],
      );

      const enrichedFields = keyFields.filter(
        (kf) => !baseProjector.projectionFields.includes(kf),
      ) as string[];

      const updatedProjector = enrichedFields.reduce(
        (p, ef) => p.project(ef as any) as any,
        baseProjector,
      );
      return [updatedProjector.expression, enrichedFields];
    } else {
      return [this.addDefinitionProjection(attributeBuilder, definition), []];
    }
  }

  static projectionFor<DEFINITION extends DynamoMapDefinition>(
    attributeBuilder: AttributeBuilder,
    projection: Projection<DEFINITION, any>,
  ): ProjectorType<DEFINITION, any> {
    return projection(new ProjectorType(attributeBuilder)) as ProjectorType<
      any,
      any
    >;
  }

  static addDefinitionProjection<DEFINITION extends DynamoMapDefinition>(
    attributeBuilder: AttributeBuilder,
    definition: DEFINITION,
  ): string {
    const keys = Object.keys(definition);
    const updatedAttributes = attributeBuilder.addNames(...keys);
    return keys.map((key) => updatedAttributes.nameFor(key)).join(',');
  }
}
