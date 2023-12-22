import { myTableClient } from './use-client';

const result = await myTableClient.transaction
  .put({
    item: { identifier: 'abc', sort: 'def', abc: { xyz: 3 } },
  })
  .then(
    myTableClient.transaction.update({
      key: { identifier: 'def', sort: '123' },
      updates: { 'abc.xyz': 9 },
    }),
  )
  .execute();
