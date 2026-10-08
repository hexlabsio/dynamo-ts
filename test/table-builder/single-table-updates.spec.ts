import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient } from '../../src';
import { Employee, Store, storeUpdatesSingleTable } from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});

const config = { client: dynamoClient, tableName: 'storeUpdatesSingleTable' };
const shop = storeUpdatesSingleTable.client(config);
const raw = TableClient.build(storeUpdatesSingleTable, config);

const s1 = { org: 'walmart', store: 's1' };
const e1 = { org: 'walmart', store: 's1', employee: 'e1' };

const storeItem = async (store: string) =>
  (await raw.get({ partition: '#ORG$walmart', sort: `#STORE$${store}` }))
    .item as Record<string, unknown> | undefined;

describe('Single table updates', () => {
  beforeEach(async () => {
    const store: Store = { ...s1, zone: 'AMER', status: 'open', name: 'One' };
    const employee: Employee = { ...e1, role: 'cashier' };
    await shop.store
      .batchPut([store])
      .and(shop.employee.batchPut([employee]))
      .execute();
  });

  it('should update attributes without touching untouched indexes', async () => {
    await shop.store.update(s1, { updates: { name: 'Renamed' } });
    expect(await storeItem('s1')).toEqual(
      expect.objectContaining({
        name: 'Renamed',
        byZone_partition: '#STORE',
        byZone_sort: '#ZONE$AMER#STATUS$open',
      }),
    );
  });

  it('should rebuild index keys when their attributes change', async () => {
    await shop.store.update(s1, {
      updates: { zone: 'EMEA', status: 'closed' },
    });
    expect(await storeItem('s1')).toEqual(
      expect.objectContaining({
        zone: 'EMEA',
        status: 'closed',
        byZone_sort: '#ZONE$EMEA#STATUS$closed',
      }),
    );
    const result = await shop.store
      .index('byZone')
      .query({}, (keys) => keys.zone('EMEA').status('closed'));
    expect(result.member.map((it) => it.store)).toEqual(['s1']);
  });

  it('should require every changing attribute of an index', async () => {
    await expect(
      shop.store.update(s1, {
        // @ts-expect-error changing status changes byZone, which also needs zone
        updates: { status: 'closed' },
      }),
    ).rejects.toThrow(
      'Updating status changes index byZone, so zone must also be provided',
    );
  });

  it('should remove an item from an index when one of its attributes is removed', async () => {
    await shop.store.update(s1, {
      updates: { zone: 'AMER', status: undefined },
    });
    const item = await storeItem('s1');
    expect(item).not.toHaveProperty('status');
    expect(item).not.toHaveProperty('byZone_partition');
    expect(item).not.toHaveProperty('byZone_sort');
  });

  it('should use key attributes when rebuilding index keys', async () => {
    await shop.employee.update(e1, { updates: { role: 'manager' } });
    const managers = await shop.employee
      .index('byRole')
      .query({ role: 'manager' }, (keys) => keys.employee('e1'));
    expect(managers.member).toEqual([
      expect.objectContaining({ employee: 'e1', role: 'manager' }),
    ]);
  });

  it('should create the item, with its keys and identifiers, if it does not exist', async () => {
    const { keys } = await shop.store.update(
      { org: 'walmart', store: 'new' },
      { updates: { zone: 'APAC', status: 'open', name: 'New' } },
    );
    expect(keys).toEqual({ partition: '#ORG$walmart', sort: '#STORE$new' });
    const { item } = await shop.store.get({ org: 'walmart', store: 'new' });
    expect(item).toEqual(
      expect.objectContaining({
        org: 'walmart',
        store: 'new',
        name: 'New',
        byZone_sort: '#ZONE$APAC#STATUS$open',
      }),
    );
  });

  it('should increment numeric attributes and return the new item', async () => {
    await shop.store.update(s1, {
      updates: { visits: 1 },
      increments: [{ key: 'visits', start: 10 }],
    });
    const result = await shop.store.update(s1, {
      updates: { visits: 5 },
      increments: [{ key: 'visits' }],
      return: 'ALL_NEW',
    });
    const visits: number | undefined = result.item.visits;
    expect(visits).toEqual(16);
  });

  it('should not increment attributes used by an index', async () => {
    await expect(
      shop.store.update(s1, {
        updates: { zone: 'AMER', status: 'open' },
        increments: [{ key: 'zone' } as any],
      }),
    ).rejects.toThrow('Cannot increment zone because index byZone uses it');
  });

  it('should apply conditions', async () => {
    await expect(
      shop.store.update(s1, {
        updates: { name: 'Nope' },
        condition: (compare) => compare().name.eq('Someone else'),
      }),
    ).rejects.toThrow('The conditional request failed');
    expect(await storeItem('s1')).toEqual(
      expect.objectContaining({ name: 'One' }),
    );
  });

  it('should only allow attributes that are not part of the key', async () => {
    await expect(
      shop.store.update(s1, {
        // @ts-expect-error store is part of the key and can't be updated
        updates: { store: 's2' },
      }),
    ).rejects.toThrow(
      "store is part of the key for store and can't be updated",
    );
    // Type check only
    const typo = () =>
      shop.store.update(s1, {
        // @ts-expect-error stores have no attribute called nme
        updates: { nme: 'typo' },
      });
    expect(typo).toBeDefined();
  });
});
