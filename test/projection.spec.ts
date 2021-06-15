import { Parser } from '../src/parsers/projection';

describe('projection parser', () => {
  type Foo = {
    bar: string;
    baz: number;
    bop: string;
  };

  it('handles projection list', () => {
    const keyParser = new Parser<Foo>(['bar', 'baz']);
    expect(keyParser.expressionAttributeNames).toEqual({
      '#bar': 'bar',
      '#baz': 'baz',
    });
    expect(keyParser.projectionAttrs).toEqual('#bar, #baz');
  });
});
