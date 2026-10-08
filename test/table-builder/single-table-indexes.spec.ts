import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient, TableDefinition } from '../../src';
import { Employee, Store, storeSingleTable } from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});

const config = { client: dynamoClient, tableName: 'storeSingleTable' };
const shop = storeSingleTable.client(config);
const raw = TableClient.build(storeSingleTable, config);

const stores: Store[] = [
  { org: 'walmart', store: 's1', zone: 'AMER', status: 'open', name: 'One' },
  { org: 'walmart', store: 's2', zone: 'AMER', status: 'closed', name: 'Two' },
  {
    org: 'walmart',
    store: 's3',
    zone: 'AMERICA',
    status: 'open',
    name: 'Three',
  },
  { org: 'walmart', store: 's4', zone: 'AMER', name: 'Four' },
  { org: 'target', store: 's5', zone: 'EMEA', status: 'open', name: 'Five' },
];

const employees: Employee[] = [
  { org: 'walmart', store: 's1', employee: 'e1', role: 'manager' },
  { org: 'walmart', store: 's2', employee: 'e10', role: 'manager' },
  { org: 'target', store: 's5', employee: 'e2', role: 'manager' },
  { org: 'walmart', store: 's1', employee: 'e3', role: 'cashier' },
];

const names = (items: { store: string }[]) =>
  items.map((it) => it.store).sort();

describe('Single table indexes', () => {
  beforeAll(async () => {
    await shop.store
      .batchPut(stores)
      .and(shop.employee.batchPut(employees))
      .execute();
  });

  describe('definition', () => {
    it('should create a global index for each index the parts use', () => {
      expect(storeSingleTable.indexes).toEqual({
        byZone: {
          global: true,
          partitionKey: 'byZone_partition',
          sortKey: 'byZone_sort',
        },
        byRole: { global: true, partitionKey: 'gsi2pk', sortKey: 'gsi2sk' },
      });
      const cloudFormation = storeSingleTable.asCloudFormation('stores');
      expect(
        cloudFormation.GlobalSecondaryIndexes!.map((it) => it.IndexName),
      ).toEqual(['byZone', 'byRole']);
      expect(
        cloudFormation.AttributeDefinitions!.map((it) => it.AttributeName),
      ).toEqual([
        'partition',
        'sort',
        'byZone_partition',
        'byZone_sort',
        'gsi2pk',
        'gsi2sk',
      ]);
    });

    it('should support indexes without sort keys', () => {
      const table = TableDefinition.singleTable(({ part }) => ({
        store: part<Store>()
          .partitionedBy('org')
          .index('byName', { partition: ['name'] }),
      }));
      expect(table.indexes).toEqual({
        byName: { global: true, partitionKey: 'byName_partition' },
      });
    });

    it('should reject configuration for an index no part uses', () => {
      expect(() =>
        TableDefinition.singleTable(
          { indexes: { nope: { partitionKey: 'x' } } },
          ({ part }) => ({ store: part<Store>().partitionedBy('org') }),
        ),
      ).toThrow('Index nope is configured but no part uses it');
    });

    it('should reject parts that disagree on whether an index has a sort key', () => {
      expect(() =>
        TableDefinition.singleTable(({ part, child }) => ({
          store: part<Store>()
            .partitionedBy('org')
            .index('shared', { partition: ['zone'], sort: ['name'] })
            .with({
              employee: child<Employee>().index('shared', {
                partition: ['role'],
              }),
            }),
        })),
      ).toThrow(
        'Every part using index shared must either have sort keys or not, check parts store, employee',
      );
    });

    it('should reject index attributes that clash with other keys', () => {
      expect(() =>
        TableDefinition.singleTable(
          { indexes: { byZone: { partitionKey: 'sort', sortKey: 'x' } } },
          ({ part }) => ({
            store: part<Store>()
              .partitionedBy('org')
              .index('byZone', { partition: [], sort: ['zone'] }),
          }),
        ),
      ).toThrow(
        'Single table key attributes must be unique, found sort more than once',
      );
    });

    it('should reject using the same index twice on a part', () => {
      expect(() =>
        TableDefinition.singleTable(({ part }) => ({
          store: part<Store>()
            .partitionedBy('org')
            .index('byZone', { partition: ['zone'] })
            // @ts-expect-error byZone is already used by this part
            .index('byZone', { partition: ['name'] }),
        })),
      ).toThrow('A part can only use index byZone once');
    });

    it('should type check index attributes and names', () => {
      TableDefinition.singleTable(({ part }) => ({
        store: part<Store>()
          .partitionedBy('org')
          // @ts-expect-error stores have no attribute called region
          .index('byRegion', { partition: ['region'] }),
      }));
      // @ts-expect-error stores have no index called byRole
      expect(() => shop.store.index('byRole')).toThrow(
        'Part store has no index called byRole',
      );
      // @ts-expect-error the byRole partition needs a role
      void shop.employee.index('byRole').query({});
    });
  });

  describe('writes', () => {
    it('should write index keys alongside the item', async () => {
      const { item } = await raw.get({
        partition: '#ORG$walmart',
        sort: '#STORE$s1',
      });
      expect(item).toEqual(
        expect.objectContaining({
          byZone_partition: '#STORE',
          byZone_sort: '#ZONE$AMER#STATUS$open',
        }),
      );
      const { item: employee } = await raw.get({
        partition: '#ORG$walmart#STORE$s1',
        sort: '#EMPLOYEE$e1',
      });
      expect(employee).toEqual(
        expect.objectContaining({
          gsi2pk: '#EMPLOYEE#ROLE$manager',
          gsi2sk: '#EMPLOYEE$e1',
        }),
      );
    });

    it('should leave items out of an index when an attribute is missing', async () => {
      const { item } = await raw.get({
        partition: '#ORG$walmart',
        sort: '#STORE$s4',
      });
      expect(item).toBeDefined();
      expect(item).not.toHaveProperty('byZone_partition');
      expect(item).not.toHaveProperty('byZone_sort');
    });

    it('should recompute index keys when an item is put again', async () => {
      await shop.store.put({ ...stores[3], status: 'open' });
      const open = await shop.store
        .index('byZone')
        .query({}, (keys) => keys.zone('AMER').status('open'));
      expect(names(open.member)).toEqual(['s1', 's4']);
      await shop.store.put(stores[3]);
    });
  });

  describe('queries', () => {
    it('should query every item in an index', async () => {
      const result = await shop.store.index('byZone').query({});
      expect(names(result.member)).toEqual(['s1', 's2', 's3', 's5']);
    });

    it('should narrow by a leading sort key without matching longer values', async () => {
      const result = await shop.store
        .index('byZone')
        .query({}, (keys) => keys.zone('AMER'));
      expect(names(result.member)).toEqual(['s1', 's2']);
    });

    it('should match a full sort key exactly', async () => {
      const result = await shop.store
        .index('byZone')
        .query({}, (keys) => keys.zone('AMER').status('open'));
      expect(result.member).toEqual([
        expect.objectContaining({ store: 's1', name: 'One' }),
      ]);
    });

    it('should query by partition attributes', async () => {
      const managers = await shop.employee
        .index('byRole')
        .query({ role: 'manager' });
      expect(managers.member.map((it) => it.employee).sort()).toEqual([
        'e1',
        'e10',
        'e2',
      ]);
      const e1 = await shop.employee
        .index('byRole')
        .query({ role: 'manager' }, (keys) => keys.employee('e1'), {
          limit: 10,
        });
      expect(e1.member.map((it) => it.employee)).toEqual(['e1']);
    });
  });
});
