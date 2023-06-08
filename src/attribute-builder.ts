import { PutItemInput } from '@aws-sdk/client-dynamodb';
import { AttributeNamer } from './naming';
import { CamelCaseKeys } from './types';

export class AttributeBuilder {
  private constructor(
    private names: Record<string, string> = {},
    private values: Record<string, unknown> = {},
  ) {}

  static create(): AttributeBuilder {
    return new AttributeBuilder();
  }

  combine(builder: AttributeBuilder): this {
    this.names = { ...this.names, ...builder.names };
    this.values = { ...this.values, ...builder.values };
    return this;
  }

  addNames(...names: string[]): this {
    this.names = names.reduce(
      (prev, name) => ({ ...prev, [name]: AttributeNamer.nameFor(name) }),
      this.names,
    );
    return this;
  }

  addValue(value: unknown): string {
    const name = AttributeNamer.uniqueName();
    this.values[name] = value;
    return `:${name}`;
  }

  buildPath(path: string): string {
    const parts = path.replace('.[', '[').split('.');
    const names = parts.map((part) =>
      part.includes('[') ? part.substring(0, part.indexOf('[')) : part,
    );
    this.addNames(...names);
    return parts
      .map((part) =>
        part.includes('[')
          ? `${this.nameFor(
              part.substring(0, part.indexOf('[')),
            )}${part.substring(part.indexOf('['))}`
          : this.nameFor(part),
      )
      .join('.');
  }

  nameFor(name: string): string {
    return `#${this.names[name]}`;
  }

  asInput(
    provided?: CamelCaseKeys<
      Pick<
        PutItemInput,
        'ExpressionAttributeNames' | 'ExpressionAttributeValues'
      >
    >,
  ): Pick<
    PutItemInput,
    'ExpressionAttributeNames' | 'ExpressionAttributeValues'
  > {
    const keys = Object.keys(this.names);
    const providedNames = Object.keys(provided?.expressionAttributeNames ?? {});
    const providedValues = Object.keys(
      provided?.expressionAttributeValues ?? {},
    );
    const valueKeys = Object.keys(this.values);
    const names =
      keys.length > 0 || providedNames.length > 0
        ? {
            ExpressionAttributeNames: {
              ...(provided?.expressionAttributeNames ?? {}),
              ...keys.reduce(
                (prev, key) => ({ ...prev, [this.nameFor(key)]: key }),
                {},
              ),
            },
          }
        : {};
    const values =
      valueKeys.length > 0 || providedValues.length > 0
        ? {
            ExpressionAttributeValues: {
              ...(provided?.expressionAttributeNames ?? {}),
              ...valueKeys.reduce(
                (prev, key) => ({ ...prev, [`:${key}`]: this.values[key] }),
                {},
              ),
            },
          }
        : {};
    return { ...names, ...values };
  }
}
