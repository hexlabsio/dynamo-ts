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

    it('handles IN expressions', () => {
        const [n1, n2, n3] = [0, 100, 200];
        const filterParser = new Parser<Foo>({ key: 'baz', comparison: ['in', [n1, n2, n3]] });
        expect(filterParser.expressionAttributeNames).toEqual({ '#baz': 'baz' });

        const [n1Key] = expectAttributeValueKV(filterParser.expressionAttributeValues, n1);
        const [n2Key] = expectAttributeValueKV(filterParser.expressionAttributeValues, n2);
        const [n3Key] = expectAttributeValueKV(filterParser.expressionAttributeValues, n3);

        expect(filterParser.expression).toEqual(`#baz IN (${n1Key},${n2Key},${n3Key})`);
    });

    it('handles begins_with expressions', () => {
        const lookupValue = 'aaa';
        const filterParser = new Parser<Foo>({ key: 'bop', comparison: ['begins_with', lookupValue] });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bop': 'bop' });

        const [attrKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lookupValue);

        expect(filterParser.expression).toEqual(`begins_with(#bop, ${attrKey})`);
    });

    it('handles contains expressions', () => {
        const lookupValue = 'abc';
        const filterParser = new Parser<Foo>({ key: 'bop', comparison: ['contains', lookupValue] });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bop': 'bop' });

        const [attrKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lookupValue);

        expect(filterParser.expression).toEqual(`contains(#bop, ${attrKey})`);
    });

    it('handles attribute_exists expressions', () => {
        const filterParser = new Parser<Foo>({ key: 'bop', comparison: 'attribute_exists' });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bop': 'bop' });
        expect(filterParser.expression).toEqual(`attribute_exists(#bop)`);
    });

    it('handles attribute_not_exists expressions', () => {
        const filterParser = new Parser<Foo>({ key: 'bop', comparison: 'attribute_not_exists' });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bop': 'bop' });
        expect(filterParser.expression).toEqual(`attribute_not_exists(#bop)`);
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

    it('handles NOT expressions', () => {
        const [lowerRange, upperRange] = [0, 100];
        const filterParser = new Parser<Foo>({
            $not: { key: 'baz', comparison: ['between', lowerRange, upperRange] }
        });
        expect(filterParser.expressionAttributeNames).toEqual({ '#baz': 'baz' });

        const [upperRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, upperRange);
        const [lowerRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lowerRange);

        expect(filterParser.expression).toEqual(`NOT (#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey})`);
    });

    it('handles AND OR NOT expressions', () => {
        const eqLookup = 123;
        const beginsWithLookup = 'aaa';
        const [lowerRange, upperRange] = [0, 100];
        const filterParser = new Parser<Foo>({
            $or:
                [{
                    $and: [
                        { key: 'bar', comparison: ['=', eqLookup] },
                        { $not: { key: 'baz', comparison: ['between', lowerRange, upperRange] } },
                    ]
                },
                { key: 'bop', comparison: ['begins_with', beginsWithLookup] }
                ]
        });
        expect(filterParser.expressionAttributeNames).toEqual({ '#bar': 'bar', '#baz': 'baz', '#bop': 'bop' });

        const [eqKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, eqLookup);
        const [beginsKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, beginsWithLookup);

        const [lowerRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, lowerRange);
        const [upperRangeKey] = expectAttributeValueKV(filterParser.expressionAttributeValues, upperRange);

        expect(filterParser.expression).toEqual(`((#bar = ${eqKey}) AND (NOT (#baz BETWEEN ${lowerRangeKey} AND ${upperRangeKey}))) OR (begins_with(#bop, ${beginsKey}))`);
    });
});

