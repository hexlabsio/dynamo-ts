import {DynamoEntry, TableClient} from "../src";
import {exampleCarTable, exampleClient} from "./example-table";

export const tableClient = TableClient.build(exampleCarTable, {client: exampleClient, logStatements: true, tableName: 'exampleCarTableDefinition'});

type Car = DynamoEntry<typeof exampleCarTable.definition>;

export async function queryTeslas(): Promise<Car[]> {
    const result = await tableClient.query({make: 'Tesla'});
    return result.member;
}

export async function queryTeslasProjected(): Promise<{model: string; year: number}[]> {
    const result = await tableClient.query({make: 'Tesla', projection: projector => projector.project('model').project('year')});
    return result.member;
}

export async function queryNissansFrom2006(): Promise<Car[]> {
    const result = await tableClient.query({make: 'Nissan', filter: compare => compare().year.eq(2006)});
    return result.member;
}

export async function queryAllNissanModelsThatBeginWith3AndFlipOrderUsingIndex(): Promise<Car[]> {
    const result = await tableClient.index('model-index').query({
        make: 'Nissan',
        model: sortKey => sortKey.beginsWith('3'),
        dynamo: { ScanIndexForward: false }
    });
    return result.member;
}

export async function queryNissansBetween2006And2022(): Promise<Car[]> {
    const result = await tableClient.query({make: 'Nissan', filter: compare => compare().year.between(2006, 2022)});
    return result.member;
}

export async function queryNissansBetween2006And2007ThatAreMetallicBlack(): Promise<Car[]> {
    const result = await tableClient.query({
        make: 'Nissan',
        filter: compare => compare().
            year.between(2006, 2007).
            and(compare().
                colour.eq('Metallic Black')
            )
    });
    return result.member;
}

export async function queryAllNissans(next?: string): Promise<{member: Car[]; next?: string}> {
    const result = await tableClient.query({make: 'Nissan', next});
    if(result.next) {
        const rest = await queryAllNissans(result.next);
        return { member: [...result.member, ...rest.member], next: rest.next };
    }
    return {member: result.member};
}


