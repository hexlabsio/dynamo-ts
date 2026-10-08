import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient } from '../../src';
import {
  Employee,
  Store,
  simpleTableDefinition,
  storeTransactionsSingleTable,
} from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const client = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});
const shop = storeTransactionsSingleTable.client({
  client,
  tableName: 'storeTransactionsSingleTable',
});
const raw = TableClient.build(storeTransactionsSingleTable, {
  client,
  tableName: 'storeTransactionsSingleTable',
});
const simple = TableClient.build(simpleTableDefinition, {
  client,
  tableName: 'simpleTableDefinition',
});

const s1 = { org: 'walmart', store: 's1' };
const e1 = { ...s1, employee: 'e1' };
const store: Store = { ...s1, zone: 'AMER', status: 'open', name: 'One' };
const employee: Employee = { ...e1, role: 'cashier' };

describe('Single table transactions', () => {
  beforeEach(async () => {
    await shop.store.delete(s1);
    await shop.employee.delete(e1);
  });

  it('should put items from several parts together, with their keys and index keys', async () => {
    await shop.store.transaction
      .put({ item: store })
      .then(shop.employee.transaction.put({ item: employee }))
      .execute();
    const { item } = await raw.get({
      partition: '#ORG$walmart',
      sort: '#STORE$s1',
    });
    expect(item).toEqual({
      ...store,
      partition: '#ORG$walmart',
      sort: '#STORE$s1',
      byZone_partition: '#STORE',
      byZone_sort: '#ZONE$AMER#STATUS$open',
    });
    expect((await shop.employee.get(e1)).item).toEqual(
      expect.objectContaining({ gsi2pk: '#EMPLOYEE#ROLE$cashier' }),
    );
  });

  it('should update, maintaining index keys and identifying attributes', async () => {
    await shop.store.transaction
      .update({
        key: s1,
        updates: { zone: 'EMEA', status: 'closed', name: 'New' },
      })
      .then(
        shop.employee.transaction.update({
          key: e1,
          updates: { role: 'manager' },
        }),
      )
      .execute();
    expect((await shop.store.get(s1)).item).toEqual(
      expect.objectContaining({
        org: 'walmart',
        store: 's1',
        byZone_sort: '#ZONE$EMEA#STATUS$closed',
      }),
    );
    const managers = await shop.employee
      .index('byRole')
      .query({ role: 'manager' }, (keys) => keys.employee('e1'));
    expect(managers.member).toHaveLength(1);
  });

  it('should apply the same update rules as update', () => {
    expect(() =>
      shop.store.transaction.update({
        key: s1,
        // @ts-expect-error changing status changes byZone, which also needs zone
        updates: { status: 'closed' },
      }),
    ).toThrow(
      'Updating status changes index byZone, so zone must also be provided',
    );
    expect(() =>
      shop.store.transaction.update({
        key: s1,
        // @ts-expect-error store is part of the key
        updates: { store: 's2' },
      }),
    ).toThrow("store is part of the key for store and can't be updated");
  });

  it('should write nothing if any condition fails', async () => {
    await shop.store.put(store);
    await expect(
      shop.employee.transaction
        .put({ item: employee })
        .then(
          shop.store.transaction.conditionCheck({
            key: s1,
            condition: (compare) => compare().status.eq('closed'),
          }),
        )
        .execute(),
    ).rejects.toMatchObject({ name: 'TransactionCanceledException' });
    expect((await shop.employee.get(e1)).item).toBeUndefined();
  });

  it('should delete', async () => {
    await shop.store.put(store);
    await shop.store.transaction
      .delete({ key: s1, condition: (compare) => compare().name.eq('One') })
      .execute();
    expect((await shop.store.get(s1)).item).toBeUndefined();
  });

  it('should combine with transactions on other tables', async () => {
    await shop.store.transaction
      .put({ item: store })
      .then(
        simple.transaction.put({
          item: { identifier: 'single-table-transaction', sort: 'x' },
        }),
      )
      .execute();
    expect((await shop.store.get(s1)).item).toBeDefined();
    expect(
      (await simple.get({ identifier: 'single-table-transaction' })).item,
    ).toBeDefined();
  });

  it('should get items from several parts together', async () => {
    await shop.store.put(store);
    await shop.employee.put(employee);
    const {
      items: [foundStore, foundEmployee, missing],
    } = await shop.store.transaction
      .get([s1], { select: ['name'] })
      .and(shop.employee.transaction.get([e1, { ...s1, employee: 'nobody' }]))
      .execute();
    const name: { name: string } | undefined = foundStore;
    const role: Employee | undefined = foundEmployee;
    expect(name).toEqual({ name: 'One' });
    expect(role).toEqual(expect.objectContaining({ role: 'cashier' }));
    expect(missing).toBeUndefined();
  });
});
