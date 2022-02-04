
import {tableClient} from "./query-filter-expressions";


export async function deleteModelS(): Promise<void> {
   await tableClient.delete({identifier: '1234', make: 'Tesla'});
}

