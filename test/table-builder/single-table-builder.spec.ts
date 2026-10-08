import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TableClient, TableDefinition, tableDefinition } from '../../src';
import { singleTableDesignDefinition } from '../tables';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo, {
  marshallOptions: { removeUndefinedValues: true },
});

export type RepoIds = {
  account: string;
  repo: string;
};

export type WorkflowIds = RepoIds & { workflow: string };

export type RunIds = WorkflowIds & { run: string };

export type JobIds = RunIds & { job: string };

export type StepIds = JobIds & { step: string };

export type LogIds = StepIds & { log: string };

export const workflowSingleTable = TableDefinition.singleTable(
  { partitionKey: 'p2', sortKey: 's2' },
  ({ part, join, child }) => ({
    repo: part<RepoIds>()
      .partitionedBy('account')
      .with({
        workflow: join<WorkflowIds>().with({
          run: child<RunIds>().with({
            job: child<JobIds>().with({
              step: join<StepIds>().with({
                log: join<LogIds>(),
              }),
            }),
          }),
        }),
      }),
  }),
);

const config = {
  client: dynamoClient,
  logStatements: true,
  tableName: 'singleTableDesignDefinition',
};

const client = workflowSingleTable.client(config);

describe('Single Table Design', () => {
  beforeAll(async () => {
    await client.workflow
      .batchPut([
        {
          account: 'account',
          repo: 'repo',
          workflow: 'workflow',
        },
        {
          account: 'account2',
          repo: 'repo2',
          workflow: 'workflow2',
        },
      ])
      .and(
        client.run.batchPut([
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run1',
          },
          {
            account: 'account2',
            repo: 'repo2',
            workflow: 'workflow2',
            run: 'run1',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
          },
        ]),
      )
      .and(
        client.job.batchPut([
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run1',
            job: 'job1',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run1',
            job: 'job2',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job3',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job4',
          },
        ]),
      )
      .and(
        client.step.batchPut([
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job4',
            step: 'step 1',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job4',
            step: 'step 2',
          },
        ]),
      )
      .and(
        client.log.batchPut([
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job4',
            step: 'step 1',
            log: 'log a',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job4',
            step: 'step 1',
            log: 'log b',
          },
          {
            account: 'account',
            repo: 'repo',
            workflow: 'workflow',
            run: 'run2',
            job: 'job4',
            step: 'step 2',
            log: 'log c',
          },
        ]),
      )
      .execute();
  });

  describe('Single table definition', () => {
    it('should describe the same table as a hand written definition', () => {
      expect(workflowSingleTable.asCloudFormation('table')).toEqual(
        singleTableDesignDefinition.asCloudFormation('table'),
      );
      expect(tableDefinition({ shop: workflowSingleTable })).toEqual(
        tableDefinition({ shop: singleTableDesignDefinition }),
      );
    });

    it('should default to partition and sort keys', () => {
      const definition = TableDefinition.singleTable(({ part }) => ({
        repo: part<RepoIds>().partitionedBy('account'),
      }));
      expect(definition.keyNames).toEqual({
        partitionKey: 'partition',
        sortKey: 'sort',
      });
      expect(definition.asSst()).toEqual({
        fields: { partition: 'string', sort: 'string' },
        primaryIndex: { hashKey: 'partition', rangeKey: 'sort' },
      });
    });

    it('should work as a raw table client definition', async () => {
      const raw = TableClient.build(workflowSingleTable, config);
      const result = await raw.get({
        p2: '#ACCOUNT$account',
        s2: '#WORKFLOW#REPO$repo#WORKFLOW$workflow',
      });
      expect(result.item).toEqual(
        expect.objectContaining({ workflow: 'workflow' }),
      );
    });

    it('should reject parts with the same name', () => {
      expect(() =>
        TableDefinition.singleTable(({ part, child }) => ({
          repo: part<RepoIds>().partitionedBy('account').with({
            workflow: child<WorkflowIds>(),
          }),
          workflow: part<WorkflowIds>().partitionedBy('account'),
        })),
      ).toThrow(
        'Single table parts must have unique names, found workflow more than once',
      );
    });

    it('should define parts from the schema, parents first', () => {
      expect(
        workflowSingleTable.parts.map((it) => ({
          name: it.prefix,
          partitions: it.part.partitions,
          sorts: it.part.sorts,
          parent: it.parents?.prefix,
        })),
      ).toEqual([
        { name: 'repo', partitions: ['account'], sorts: ['repo'] },
        {
          name: 'workflow',
          partitions: ['account'],
          sorts: ['repo', 'workflow'],
          parent: 'repo',
        },
        {
          name: 'run',
          partitions: ['account', 'repo', 'workflow'],
          sorts: ['run'],
        },
        {
          name: 'job',
          partitions: ['account', 'repo', 'workflow', 'run'],
          sorts: ['job'],
        },
        {
          name: 'step',
          partitions: ['account', 'repo', 'workflow', 'run'],
          sorts: ['job', 'step'],
          parent: 'job',
        },
        {
          name: 'log',
          partitions: ['account', 'repo', 'workflow', 'run'],
          sorts: ['job', 'step', 'log'],
          parent: 'step',
        },
      ]);
    });

    it('should reject invalid schemas at compile time', () => {
      TableDefinition.singleTable(({ part, join }) => ({
        // @ts-expect-error repos has no attribute called workflows
        repo: part<RepoIds>().partitionedBy('account').with({
          workflows: join<WorkflowIds>(),
        }),
      }));
      TableDefinition.singleTable(({ part, child }) => ({
        // @ts-expect-error the child is missing its parent's account key
        repo: part<RepoIds>().partitionedBy('account').with({
          workflow: child<{ repo: string; workflow: string }>(),
        }),
      }));
      TableDefinition.singleTable(({ part }) => ({
        // @ts-expect-error the part name must be an attribute of the part
        repos: part<RepoIds>().partitionedBy('account'),
      }));
      TableDefinition.singleTable(({ part }) => ({
        // @ts-expect-error accounts is not an attribute of the part
        repo: part<RepoIds>().partitionedBy('accounts'),
      }));
      const client = workflowSingleTable.client(config);
      // @ts-expect-error there is no part called nope
      expect(client.nope).toBeUndefined();
    });
  });

  it('should match sort keys precisely when querying', async () => {
    const workflow = { account: 'account3', repo: 'repo', workflow: 'wf' };
    await client.run
      .batchPut([
        { ...workflow, run: 'run1' },
        { ...workflow, run: 'run10' },
      ])
      .and(
        client.job.batchPut([
          { ...workflow, run: 'run1', job: 'job1' },
          { ...workflow, run: 'run1', job: 'job10' },
        ]),
      )
      .execute();
    const runs = await client.run.query(workflow, (keys) => keys.run('run1'));
    expect(runs.member.map((it) => it.run)).toEqual(['run1']);
    const allRuns = await client.run.query(workflow);
    expect(allRuns.member.map((it) => it.run)).toEqual(['run1', 'run10']);
    const jobs = await client.job.query({ ...workflow, run: 'run1' }, (keys) =>
      keys.job('job1'),
    );
    expect(jobs.member.map((it) => it.job)).toEqual(['job1']);
  });

  it('should return generated keys for single table put', async () => {
    const result = await client.job.put({
      account: 'account5',
      repo: 'repo5',
      workflow: 'workflow',
      run: 'run2',
      job: 'x',
    });

    expect(result.keys).toEqual({
      p2: '#ACCOUNT$account5#REPO$repo5#WORKFLOW$workflow#RUN$run2',
      s2: '#JOB$x',
    });
  });

  it('should query child joined to child', async () => {
    const result = await client.job.query({
      account: 'account',
      repo: 'repo',
      workflow: 'workflow',
      run: 'run2',
    });
    expect(result.member[0]).toEqual({
      p2: '#ACCOUNT$account#REPO$repo#WORKFLOW$workflow#RUN$run2',
      s2: '#JOB$job3',
      account: 'account',
      job: 'job3',
      repo: 'repo',
      run: 'run2',
      workflow: 'workflow',
    });
  });

  it('should query child joined to child two levels', async () => {
    const result = await client.step.queryWithParents(
      {
        account: 'account',
        repo: 'repo',
        workflow: 'workflow',
        run: 'run2',
      },
      { returnConsumedCapacity: 'TOTAL' },
    );
    // One query for the jobs and one for their steps
    expect(result.consumedCapacity!.CapacityUnits).toEqual(1);
    expect(result.member[1]).toEqual({
      item: {
        p2: '#ACCOUNT$account#REPO$repo#WORKFLOW$workflow#RUN$run2',
        s2: '#JOB$job4',
        account: 'account',
        repo: 'repo',
        workflow: 'workflow',
        run: 'run2',
        job: 'job4',
      },
      member: [
        {
          p2: '#ACCOUNT$account#REPO$repo#WORKFLOW$workflow#RUN$run2',
          s2: '#STEP#JOB$job4#STEP$step 1',
          account: 'account',
          repo: 'repo',
          workflow: 'workflow',
          run: 'run2',
          job: 'job4',
          step: 'step 1',
        },
        {
          p2: '#ACCOUNT$account#REPO$repo#WORKFLOW$workflow#RUN$run2',
          s2: '#STEP#JOB$job4#STEP$step 2',
          account: 'account',
          repo: 'repo',
          workflow: 'workflow',
          run: 'run2',
          job: 'job4',
          step: 'step 2',
        },
      ],
    });
  });

  it('should page through parents with their children', async () => {
    const partition = {
      account: 'account',
      repo: 'repo',
      workflow: 'workflow',
      run: 'run2',
    };
    const page1 = await client.step.queryWithParents(partition, { limit: 1 });
    expect(page1.member.map((it) => it.item.job)).toEqual(['job3']);
    expect(page1.member[0].member).toEqual([]);
    expect(page1.next).toBeDefined();

    const page2 = await client.step.queryWithParents(partition, {
      limit: 1,
      next: page1.next,
    });
    expect(page2.member.map((it) => it.item.job)).toEqual(['job4']);
    expect(page2.member[0].member.map((it) => it.step)).toEqual([
      'step 1',
      'step 2',
    ]);

    const remaining = page2.next
      ? await client.step.queryWithParents(partition, {
          limit: 1,
          next: page2.next,
        })
      : { member: [], next: undefined };
    expect(remaining.member).toEqual([]);
    expect(remaining.next).toBeUndefined();
  });

  it('should query all pages of parents with their children', async () => {
    const result = await client.log.queryAllWithParents(
      { account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2' },
      { returnConsumedCapacity: 'TOTAL' },
    );
    const summary = result.member.map((job) => ({
      job: job.item.job,
      steps: job.member.map((step) => ({
        step: step.item.step,
        logs: step.member.map((log) => log.log),
      })),
    }));
    expect(summary).toEqual([
      { job: 'job3', steps: [] },
      {
        job: 'job4',
        steps: [
          { step: 'step 1', logs: ['log a', 'log b'] },
          { step: 'step 2', logs: ['log c'] },
        ],
      },
    ]);
    expect(result.count).toEqual(2);
    expect(result.consumedCapacity!.CapacityUnits).toBeGreaterThan(0);
  });

  it('should group three levels of joined parts', async () => {
    const result = await client.log.queryWithParents({
      account: 'account',
      repo: 'repo',
      workflow: 'workflow',
      run: 'run2',
    });
    const summary = result.member.map((job) => ({
      job: job.item.job,
      steps: job.member.map((step) => ({
        step: step.item.step,
        logs: step.member.map((log) => log.log),
      })),
    }));
    expect(summary).toEqual([
      { job: 'job3', steps: [] },
      {
        job: 'job4',
        steps: [
          { step: 'step 1', logs: ['log a', 'log b'] },
          { step: 'step 2', logs: ['log c'] },
        ],
      },
    ]);
  });

  it('should only return children of the parents in the page', async () => {
    const result = await client.log.queryWithParents(
      { account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2' },
      { limit: 1 },
    );
    expect(result.member).toEqual([
      expect.objectContaining({
        item: expect.objectContaining({ job: 'job3' }),
        member: [],
      }),
    ]);
  });

  it('should query children for parent', async () => {
    const result = await client.run.queryWithParents({
      account: 'account',
      repo: 'repo',
      workflow: 'workflow',
    });
    expect(result.member).toEqual([
      {
        p2: '#ACCOUNT$account#REPO$repo#WORKFLOW$workflow',
        s2: '#RUN$run1',
        account: 'account',
        repo: 'repo',
        run: 'run1',
        workflow: 'workflow',
      },
      {
        p2: '#ACCOUNT$account#REPO$repo#WORKFLOW$workflow',
        s2: '#RUN$run2',
        account: 'account',
        repo: 'repo',
        run: 'run2',
        workflow: 'workflow',
      },
    ]);
  });

  it('should delete child', async () => {
    const key = {
      account: 'account2',
      workflow: 'workflow2',
      repo: 'repo2',
    };
    const result = await client.run.queryWithParents(key);

    expect(result.member.length).toEqual(1);

    await client.run.delete({ ...key, run: 'run1' });

    const result2 = await client.run.queryWithParents(key);

    expect(result2.member.length).toEqual(0);
  });
});
