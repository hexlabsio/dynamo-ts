import { AttributeBuilder } from './attribute-builder';
import { JsonPath, ValueAtJsonPath } from './types/json-path';

type TupleKeys<P extends string> = P extends `${infer A}.${infer TAIL}`
  ? [A, ...TupleKeys<TAIL>]
  : P extends `${infer A}`
    ? [A]
    : never;

type FromKeys<KEYS extends any[], V> = KEYS extends [infer K, ...infer KS]
  ? K extends `[${number}]`
    ? FromKeys<KS, V>[] | undefined
    : { [KEY in K extends string ? K : never]: FromKeys<KS, V> }
  : KEYS extends [infer K3]
    ? { [KEY in K3 extends string ? K3 : never]: V }
    : V;
interface Projector<T, PROJECTED = {}> {
  project<PATH extends JsonPath<T>>(
    path: PATH,
  ): Projector<
    T,
    PROJECTED &
    FromKeys<TupleKeys<PATH>, ValueAtJsonPath<PATH, T>>
  >;
}

export class ProjectorType<TableType, PROJECTED = {}>
  implements Projector<TableType, PROJECTED>
{
  constructor(
    private readonly attributeBuilder: AttributeBuilder,
    readonly expression: string = '',
    readonly projectionFields: (keyof TableType)[] = [],
  ) {}

  project<PATH extends JsonPath<TableType>>(
    path: PATH,
  ): Projector<
    TableType,
    PROJECTED &
    ValueAtJsonPath<PATH, TableType>
  > {
    const projectionExpression = this.attributeBuilder.buildPath(path);
    return new ProjectorType(
      this.attributeBuilder,
      this.expression
        ? `${this.expression},${projectionExpression}`
        : projectionExpression,
      [path, ...this.projectionFields] as any,
    ) as any;
  }
}

export type Projection<TableType, R> = (
  projector: Projector<TableType>,
) => Projector<TableType, R>;

export class ProjectionHandler {
  static projectionExpressionFor(
    attributeBuilder: AttributeBuilder,
    projection: Projection<any, any>,
  ): string {
    return this.projectionFor(attributeBuilder, projection).expression;
  }

  static projectionFor(
    attributeBuilder: AttributeBuilder,
    projection: Projection<any, any>,
  ): ProjectorType<any, any> {
    const p = new ProjectorType(attributeBuilder)
    return projection(p) as any;
  }
}
