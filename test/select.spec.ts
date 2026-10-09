import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { Selected, TableClient, TableDefinition } from '../src';
import { Account, Invoice, invoiceSelectSingleTable } from './tables';

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
function assertType<T extends true>(): void {}

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const client = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});

type Car = {
  make: string;
  identifier: string;
  model: string;
  year: number;
  specs?: { seats: number; doors: number };
  tags?: string[];
  owners?: { name: string; years: number[] }[];
};

const carTable = TableDefinition.ofType<Car>()
  .withPartitionKey('make')
  .withSortKey('identifier');
const cars = TableClient.build(carTable, { client, tableName: 'selectCars' });

const billing = invoiceSelectSingleTable.client({
  client,
  tableName: 'invoiceSelectSingleTable',
});

describe('Select', () => {
  beforeAll(async () => {
    await dynamo
      .createTable(
        carTable.asCloudFormation('selectCars', {
          BillingMode: 'PAY_PER_REQUEST',
        }) as any,
      )
      .catch(() => undefined);
    await cars
      .batchPut([
        {
          make: 'Tesla',
          identifier: '1',
          model: 'S',
          year: 2020,
          specs: { seats: 5, doors: 4 },
          tags: ['electric', 'fast'],
          owners: [{ name: 'Ada', years: [2020, 2021] }],
        },
        { make: 'Tesla', identifier: '2', model: '3', year: 2021 },
        { make: 'Tesla', identifier: '3', model: 'X', year: 2022 },
      ])
      .execute();
    const accounts: Account[] = [{ tenant: 't1', account: 'a1', name: 'Acme' }];
    const invoices: Invoice[] = [
      {
        tenant: 't1',
        account: 'a1',
        invoice: 'i1',
        due: '2026-01-01',
        status: 'open',
        amount: 10,
      },
      {
        tenant: 't1',
        account: 'a1',
        invoice: 'i2',
        due: '2026-02-01',
        status: 'paid',
        amount: 20,
      },
    ];
    await billing.account
      .batchPut(accounts)
      .and(billing.invoice.batchPut(invoices))
      .execute();
  });

  describe('types', () => {
    it('should keep optional attributes optional and merge nested paths', () => {
      assertType<
        Same<
          Selected<Car, ['model', 'specs.seats', 'specs.doors']>,
          { model: string; specs?: { seats: number; doors: number } }
        >
      >();
      assertType<Same<Selected<Car, ['tags.[0]']>, { tags?: string[] }>>();
      assertType<
        Same<
          Selected<Car, ['owners.[0].name']>,
          { owners?: { name: string }[] }
        >
      >();
    });

    it('should only accept paths in the table', () => {
      // @ts-expect-error cars have no colour
      void cars.get({ make: 'Tesla', identifier: '1' }, { select: ['colour'] });
      void cars.get(
        { make: 'Tesla', identifier: '1' },
        // @ts-expect-error specs has no wheels
        { select: ['specs.wheels'] },
      );
    });
  });

  it('should read nested attributes and list elements', async () => {
    const { item } = await cars.get(
      { make: 'Tesla', identifier: '1' },
      { select: ['model', 'specs.seats', 'tags.[1]', 'owners.[0].years.[1]'] },
    );
    expect(item).toEqual({
      model: 'S',
      specs: { seats: 5 },
      tags: ['fast'],
      owners: [{ years: [2021] }],
    });
  });

  it('should leave out optional attributes an item does not have', async () => {
    const { item } = await cars.get(
      { make: 'Tesla', identifier: '2' },
      { select: ['model', 'specs.seats'] },
    );
    expect(item).toEqual({ model: '3' });
  });

  it('should accept overlapping and repeated paths', async () => {
    const { item } = await cars.get(
      { make: 'Tesla', identifier: '1' },
      { select: ['specs', 'specs.seats', 'model', 'model'] },
    );
    expect(item).toEqual({ model: 'S', specs: { seats: 5, doors: 4 } });
  });

  it('should page through queryAll without selecting the key attributes', async () => {
    const result = await cars.queryAll(
      { make: 'Tesla' },
      { select: ['model'], limit: 2 },
    );
    expect(result.member).toEqual([{ model: 'S' }, { model: '3' }]);
    expect(result.next).toBeDefined();
    const rest = await cars.queryAll(
      { make: 'Tesla' },
      { select: ['model'], limit: 2, next: result.next },
    );
    expect(rest.member).toEqual([{ model: 'X' }]);
  });

  it('should group queryWithParents results without selecting the key attributes', async () => {
    const { member } = await billing.invoice.queryWithParents(
      { tenant: 't1' },
      { select: ['name', 'amount'] },
    );
    assertType<
      Same<
        typeof member,
        { item: { name: string }; member: { amount: number }[] }[]
      >
    >();
    expect(member).toEqual([
      { item: { name: 'Acme' }, member: [{ amount: 10 }, { amount: 20 }] },
    ]);
  });
});
