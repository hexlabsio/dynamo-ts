import {tableClient} from "./query-filter-expressions";
import { Car } from "./example-table";

export async function updateModelS(): Promise<void> {
   await tableClient.update({key: {identifier: '1234', make: 'Tesla'}, updates: {year: 2022, colour: undefined}});
}

export async function updateModelSReturnOld(): Promise<Car> {
   const result = await tableClient.update({key: {identifier: '1234', make: 'Tesla'}, updates: {year: 2022, colour: undefined}, return: 'ALL_OLD'});
   return result.item
}

export async function atomicUpdate(): Promise<void> {
   await tableClient.update({key: {identifier: '1234', make: 'Tesla'}, updates: {year: 1, model: 'Another Model'}, increments: [{key: 'year', start: 2020}]});
}
