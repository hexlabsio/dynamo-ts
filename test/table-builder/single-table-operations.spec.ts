import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import {
  Employee,
  Store,
  storeOperationsSingleTable,
  ticketSingleTable,
} from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const client = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});
const shop = storeOperationsSingleTable.client({
  client,
  tableName: 'storeOperationsSingleTable',
});
const tickets = ticketSingleTable.client({
  client,
  tableName: 'ticketSingleTable',
});

const stores: Store[] = [
  { org: 'walmart', store: 's1', zone: 'AMER', status: 'open', name: 'One' },
  { org: 'walmart', store: 's2', zone: 'EMEA', status: 'open', name: 'Two' },
];
const employees: Employee[] = [
  { org: 'walmart', store: 's1', employee: 'e1', role: 'manager' },
  { org: 'walmart', store: 's1', employee: 'e2', role: 'cashier' },
  { org: 'walmart', store: 's1', employee: 'e3', role: 'cashier' },
];

describe('Single table operations', () => {
  beforeAll(async () => {
    await shop.store
      .batchPut(stores)
      .and(shop.employee.batchPut(employees))
      .execute();
  });

  it('should batch get from several parts of the same table', async () => {
    const {
      items: [foundStores, foundEmployees],
    } = await shop.store
      .batchGet([
        { org: 'walmart', store: 's1' },
        { org: 'walmart', store: 's2' },
      ])
      .and(
        shop.employee.batchGet(
          [{ org: 'walmart', store: 's1', employee: 'e1' }],
          {
            select: ['role'],
          },
        ),
      )
      .execute();
    const typedStores: Store[] = foundStores;
    const roles: { role: string }[] = foundEmployees;
    expect(typedStores.map((it) => it.name).sort()).toEqual(['One', 'Two']);
    expect(roles).toEqual([{ role: 'manager' }]);
  });

  it('should batch delete, combined with other parts and writes', async () => {
    const extra: Employee = {
      org: 'walmart',
      store: 's2',
      employee: 'temp',
      role: 'temp',
    };
    await shop.employee.batchPut([extra]).execute();
    await shop.employee
      .batchDelete([{ org: 'walmart', store: 's2', employee: 'temp' }])
      .and(shop.store.batchPut([{ ...stores[1], name: 'Two again' }]))
      .execute();
    expect(
      (
        await shop.employee.get({
          org: 'walmart',
          store: 's2',
          employee: 'temp',
        })
      ).item,
    ).toBeUndefined();
    expect(
      (await shop.store.get({ org: 'walmart', store: 's2' })).item?.name,
    ).toEqual('Two again');
  });

  it('should query every page', async () => {
    const result = await shop.employee.queryAll(
      { org: 'walmart', store: 's1' },
      undefined,
      { limit: 1 },
    );
    expect(result.member.map((it) => it.employee)).toEqual(['e1', 'e2', 'e3']);
    expect(result.count).toEqual(3);
    expect(result).not.toHaveProperty('next');
  });

  it('should query every page of an index', async () => {
    const cashiers = await shop.employee
      .index('byRole')
      .queryAll({ role: 'cashier' }, undefined, { limit: 1 });
    expect(cashiers.member.map((it) => it.employee)).toEqual(['e2', 'e3']);
    const zones = await shop.store
      .index('byZone')
      .queryAll({}, (keys) => keys.zone('AMER'));
    expect(zones.member.map((it) => it.store)).toEqual(['s1']);
  });

  it("should scan only the part's items", async () => {
    const allStores = await shop.store.scanAll();
    expect(allStores.member.map((it) => it.store).sort()).toEqual(['s1', 's2']);
    const allEmployees = await shop.employee.scanAll({
      filter: (compare) => compare().role.eq('cashier'),
    });
    expect(allEmployees.member.map((it) => it.employee).sort()).toEqual([
      'e2',
      'e3',
    ]);
    const page = await shop.store.scan({ limit: 100 });
    expect(page.member.every((it) => it.zone !== undefined)).toBe(true);
  });

  it('should use the types of key attributes', async () => {
    await tickets.ticket.put({ project: 'p', ticket: 7, title: 'Bug' });
    await tickets.ticket.put({ project: 'p', ticket: 70, title: 'Feature' });
    const { item } = await tickets.ticket.get({ project: 'p', ticket: 7 });
    expect(item?.title).toEqual('Bug');
    const found = await tickets.ticket.query({ project: 'p' }, (keys) =>
      keys.ticket(7),
    );
    expect(found.member.map((it) => it.ticket)).toEqual([7]);
    // @ts-expect-error ticket is a number
    void tickets.ticket.get({ project: 'p', ticket: '7' });
  });
});
