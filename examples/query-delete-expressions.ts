
import {tableClient} from "./query-filter-expressions";


export async function deleteModelS(): Promise<void> {
   await tableClient.delete({identifier: '1234', make: 'Tesla'});
}

export async function conditionallyDeleteByYear(): Promise<void> {
   await tableClient.delete({identifier: '1234', make: 'Tesla'}, {condition: (compare) => compare().year.eq(2021)});
}
