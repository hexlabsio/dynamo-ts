import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient, TableDefinition } from '../../src';
import { Account, Invoice, invoiceSingleTable } from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});
const config = { client: dynamoClient, tableName: 'invoiceSingleTable' };
const billing = invoiceSingleTable.client(config);
const raw = TableClient.build(invoiceSingleTable, config);

const accounts: Account[] = [
  { tenant: 't1', account: 'a1', name: 'Zed' },
  { tenant: 't1', account: 'a2', name: 'Acme' },
];
const invoices: Invoice[] = [
  {
    tenant: 't1',
    account: 'a1',
    invoice: 'i1',
    due: '2026-12-01',
    status: 'open',
    amount: 10,
  },
  {
    tenant: 't1',
    account: 'a1',
    invoice: 'i2',
    due: '2026-10-01',
    status: 'paid',
    amount: 20,
  },
  {
    tenant: 't1',
    account: 'a2',
    invoice: 'i3',
    due: '2026-11-01',
    status: 'open',
    amount: 30,
  },
  { tenant: 't1', account: 'a2', invoice: 'i4', due: '2026-09-01', amount: 40 },
  {
    tenant: 't2',
    account: 'a9',
    invoice: 'i9',
    due: '2026-01-01',
    status: 'open',
    amount: 90,
  },
];

describe('Single table local indexes', () => {
  beforeAll(async () => {
    await billing.account
      .batchPut(accounts)
      .and(billing.invoice.batchPut(invoices))
      .execute();
  });

  describe('definition', () => {
    it('should create local indexes on the table partition key', () => {
      expect(invoiceSingleTable.indexes).toEqual({
        byName: {
          global: false,
          partitionKey: 'partition',
          sortKey: 'byName_sort',
        },
        byLabel: {
          global: false,
          partitionKey: 'partition',
          sortKey: 'byLabel_sort',
        },
        byDue: {
          global: false,
          partitionKey: 'partition',
          sortKey: 'byDue_sort',
        },
        byStatus: { global: false, partitionKey: 'partition', sortKey: 'lsi1' },
      });
      const cloudFormation = invoiceSingleTable.asCloudFormation('billing');
      expect(cloudFormation.GlobalSecondaryIndexes).toBeUndefined();
      expect(cloudFormation.LocalSecondaryIndexes).toContainEqual({
        IndexName: 'byDue',
        KeySchema: [
          { KeyType: 'HASH', AttributeName: 'partition' },
          { KeyType: 'RANGE', AttributeName: 'byDue_sort' },
        ],
        Projection: { ProjectionType: 'ALL' },
      });
      expect(
        invoiceSingleTable.asTerraform('billing').local_secondary_index,
      ).toContainEqual({
        name: 'byStatus',
        range_key: 'lsi1',
        projection_type: 'ALL',
      });
    });

    it('should reject parts that disagree on whether an index is local', () => {
      expect(() =>
        TableDefinition.singleTable(({ part, join }) => ({
          account: part<Account>()
            .partitionedBy('tenant')
            .localIndex('shared', { sort: ['name'] })
            .with({
              invoice: join<Invoice>().index('shared', {
                partition: ['account'],
                sort: ['due'],
              }),
            }),
        })),
      ).toThrow(
        'Every part using index shared must agree on whether it is local or global, check parts account, invoice',
      );
    });

    it('should reject a partition key attribute for a local index', () => {
      expect(() =>
        TableDefinition.singleTable(
          { indexes: { byName: { partitionKey: 'x', sortKey: 'y' } } },
          ({ part }) => ({
            account: part<Account>()
              .partitionedBy('tenant')
              .localIndex('byName', { sort: ['name'] }),
          }),
        ),
      ).toThrow(
        "Index byName is local so it uses the table's partition key, only configure its sortKey",
      );
    });

    it('should allow at most five local indexes', () => {
      expect(() =>
        TableDefinition.singleTable(({ part }) => ({
          account: part<Account>()
            .partitionedBy('tenant')
            .localIndex('l1', { sort: ['name'] })
            .localIndex('l2', { sort: ['name'] })
            .localIndex('l3', { sort: ['name'] })
            .localIndex('l4', { sort: ['name'] })
            .localIndex('l5', { sort: ['name'] })
            .localIndex('l6', { sort: ['name'] }),
        })),
      ).toThrow('A table can have at most 5 local indexes, found 6');
    });

    it('should type check local indexes', () => {
      TableDefinition.singleTable(({ part }) => ({
        account: part<Account>()
          .partitionedBy('tenant')
          // @ts-expect-error local indexes need at least one sort attribute
          .localIndex('empty', { sort: [] }),
      }));
      // @ts-expect-error local index queries need the part's partition attributes
      void billing.invoice.index('byDue').query({});
    });
  });

  describe('writes', () => {
    it('should write local index sort keys starting with the part name', async () => {
      const { item: account } = await raw.get({
        partition: '#TENANT$t1',
        sort: '#ACCOUNT$a2',
      });
      expect(account).toEqual(
        expect.objectContaining({ byName_sort: '#ACCOUNT#NAME$Acme' }),
      );
      const { item: invoice } = await raw.get({
        partition: '#TENANT$t1',
        sort: '#INVOICE#ACCOUNT$a1#INVOICE$i1',
      });
      expect(invoice).toEqual(
        expect.objectContaining({
          byDue_sort: '#INVOICE#DUE$2026-12-01',
          lsi1: '#INVOICE#STATUS$open#DUE$2026-12-01',
        }),
      );
      const { item: unpaid } = await raw.get({
        partition: '#TENANT$t1',
        sort: '#INVOICE#ACCOUNT$a2#INVOICE$i4',
      });
      expect(unpaid).not.toHaveProperty('lsi1');
    });
  });

  describe('queries', () => {
    it("should query a part's items in a partition in index order", async () => {
      const result = await billing.invoice
        .index('byDue')
        .query({ tenant: 't1' }, undefined, { consistentRead: true });
      expect(result.member.map((it) => it.invoice)).toEqual([
        'i4',
        'i2',
        'i3',
        'i1',
      ]);
      const names = await billing.account
        .index('byName')
        .query({ tenant: 't1' });
      expect(names.member.map((it) => it.name)).toEqual(['Acme', 'Zed']);
    });

    it('should only return items of the queried part from a shared local index', async () => {
      const accountsByLabel = await billing.account
        .index('byLabel')
        .query({ tenant: 't1' });
      expect(accountsByLabel.member.map((it) => it.account)).toEqual([
        'a2',
        'a1',
      ]);
      const invoicesByLabel = await billing.invoice
        .index('byLabel')
        .query({ tenant: 't1' });
      expect(invoicesByLabel.member.map((it) => it.invoice)).toEqual([
        'i1',
        'i2',
        'i3',
        'i4',
      ]);
    });

    it('should narrow by leading sort attributes', async () => {
      const open = await billing.invoice
        .index('byStatus')
        .query({ tenant: 't1' }, (keys) => keys.status('open'));
      expect(open.member.map((it) => it.invoice)).toEqual(['i3', 'i1']);
      const exact = await billing.invoice
        .index('byDue')
        .query({ tenant: 't1' }, (keys) => keys.due('2026-10-01'));
      expect(exact.member.map((it) => it.invoice)).toEqual(['i2']);
    });
  });

  describe('updates', () => {
    it('should rebuild local index keys', async () => {
      const key = { tenant: 't1', account: 'a2', invoice: 'i3' };
      await billing.invoice.update(key, {
        updates: { status: 'paid', due: '2026-11-15' },
      });
      const { item } = await billing.invoice.get(key);
      expect(item).toEqual(
        expect.objectContaining({
          byDue_sort: '#INVOICE#DUE$2026-11-15',
          lsi1: '#INVOICE#STATUS$paid#DUE$2026-11-15',
        }),
      );
      await expect(
        // @ts-expect-error due is also used by byStatus, which needs status too
        billing.invoice.update(key, { updates: { due: '2026-12-31' } }),
      ).rejects.toThrow(
        'Updating due changes index byStatus, so status must also be provided',
      );
      await billing.invoice.update(key, {
        updates: { status: undefined, due: '2026-11-15' },
      });
      const { item: removed } = await billing.invoice.get(key);
      expect(removed).not.toHaveProperty('lsi1');
      expect(removed).toEqual(
        expect.objectContaining({ byDue_sort: '#INVOICE#DUE$2026-11-15' }),
      );
    });
  });
});
