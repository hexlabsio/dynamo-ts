import { DynamoTypeFrom } from '../src';
import {exampleCarTable} from "./example-table";
import {tableClient} from "./query-filter-expressions";

type Car = DynamoTypeFrom<typeof exampleCarTable>;

export async function putModelSReturnOldValues(): Promise<Car | undefined> {
    const result = await tableClient.put({identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white'}, { returnValues: 'ALL_OLD' })
    return result.item
}


export async function conditionallyPutIfNotExistsModelS(): Promise<Car | undefined> {
    const result = await tableClient.put(
      {identifier: '1234', make: 'Tesla', model: 'Model S', year: 2022, colour: 'white'},
      {condition: compare => compare().notExists('identifier'), returnValues: 'ALL_OLD'})
    return result.item
}

