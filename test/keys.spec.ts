import { Parser } from "../src/parsers/keys";

import { expectAttributeValueKV } from "./testUtil";

describe('key parser', () => {
    type Foo = {
        bar: string,
        baz: number,
        bop: string;
    };

    it('handles simple expressions', () => {
        const lookupValue = '123';
        const keyParser = new Parser<Foo>({ key: 'bar', comparison: ['=', lookupValue] });
        expect(keyParser.expressionAttributeNames).toEqual({ '#bar': 'bar' });

        const attrKV = expectAttributeValueKV(keyParser.expressionAttributeValues, lookupValue);

        expect(keyParser.expression).toEqual(`#bar = ${attrKV![0]}`);

    });

    it('handles range expressions', () => {
        const [lowerRange, upperRange] = [0, 100];
        const keyParser = new Parser<Foo>({ key: 'baz', comparison: ['between', lowerRange, upperRange] });
        expect(keyParser.expressionAttributeNames).toEqual({ '#baz': 'baz' });

        const [lowerRangeKey] = expectAttributeValueKV(keyParser.expressionAttributeValues, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(keyParser.expressionAttributeValues, upperRange);

        expect(keyParser.expression).toEqual(`#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey}`);
    });

    it('handles AND expressions', () => {
        const eqLookup = 123;
        const [lowerRange, upperRange] = [0, 100];
        const keyParser = new Parser<Foo>({
            $and: [
                { key: 'bar', comparison: ['=', eqLookup] },
                { key: 'baz', comparison: ['between', lowerRange, upperRange] },
            ]
        });
        expect(keyParser.expressionAttributeNames).toEqual({ '#bar': 'bar', '#baz': 'baz' });

        const [eqKey] = expectAttributeValueKV(keyParser.expressionAttributeValues, eqLookup);

        const [lowerRangeKey] = expectAttributeValueKV(keyParser.expressionAttributeValues, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(keyParser.expressionAttributeValues, upperRange);

        expect(keyParser.expression).toEqual(`(#bar = ${eqKey}) AND (#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey})`);
    }); 
});

