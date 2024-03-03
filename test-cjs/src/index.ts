import { TableDefinition } from '@hexlabs/dynamo-ts';

(async () => {
  const table = TableDefinition.ofType<{a: string, b: number}>().withPartitionKey('a');
  console.log(table.asCloudFormation('test'));
})()
