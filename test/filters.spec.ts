import { Parser } from "../src/parsers/filters";

import { expectAttributeValueKV } from "./testUtil";

describe('filter parser', () => {
    type Foo = {
        bar: string,
        baz: number,
        bop: string;
    };

    it('handles simple expressions', () => {
        const lookupValue = '123';
        const filterParser = new Parser<Foo>({ key: 'bar', comparison: ['=', lookupValue] });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bar': 'bar' });

        const [attrKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lookupValue);

        expect(filterParser.expression).toEqual(`#bar = ${attrKey}`);

    });

    it('handles range expressions', () => {
        const [lowerRange, upperRange] = [0, 100];
        const filterParser = new Parser<Foo>({ key: 'baz', comparison: ['between', lowerRange, upperRange] });
        expect(filterParser.expressionAttributeNames).toEqual({ '#baz': 'baz' });

        const [lowerRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, upperRange);

        expect(filterParser.expression).toEqual(`#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey}`);
    });

    it('handles begins_with expressions', () => {
        const lookupValue = 'aaa'
        const filterParser = new Parser<Foo>({ key: 'bop', comparison: ['begins_with', lookupValue] });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bop': 'bop' });

        const [attrKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lookupValue);

        expect(filterParser.expression).toEqual(`begins_with(#bop, ${attrKey})`);
    });

    it('handles AND expressions', () => {
        const eqLookup = 123;
        const [lowerRange, upperRange] = [0, 100];
        const filterParser = new Parser<Foo>({
            $and: [
                { key: 'bar', comparison: ['=', eqLookup] },
                { key: 'baz', comparison: ['between', lowerRange, upperRange] },
            ]
        });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bar': 'bar', '#baz': 'baz' });

        const [eqKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, eqLookup);

        const [lowerRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, upperRange);

        expect(filterParser.expression).toEqual(`(#bar = ${eqKey}) AND (#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey})`);
    });
    
    it('handles AND OR expressions', () => {
        const eqLookup = 123;
        const beginsWithLookup = 'aaa';
        const [lowerRange, upperRange] = [0, 100];
        const filterParser = new Parser<Foo>({
            $or: 
            [{$and: [
                { key: 'bar', comparison: ['=', eqLookup] },
                { key: 'baz', comparison: ['between', lowerRange, upperRange] },
            ]},
            { key: 'bop', comparison: ['begins_with', beginsWithLookup] } 
        ]
        });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bar': 'bar', '#baz': 'baz', '#bop' : 'bop' });

        const [eqKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, eqLookup);
        const [beginsKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, beginsWithLookup);

        const [lowerRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, upperRange);

        expect(filterParser.expression).toEqual(`((#bar = ${eqKey}) AND (#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey})) OR (begins_with(#bop, ${beginsKey}))`);
    });
});

