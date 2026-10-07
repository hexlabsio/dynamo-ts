import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TablePartClient, TablePartInfo } from '../../src';
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

export const repositoryTable = TablePartInfo.from<RepoIds>().withKeys(
  'account',
  'repo',
);

export const workflowTable = repositoryTable
  .joinPart<WorkflowIds>()
  .withKey('workflow');

export const workflowRunTable = workflowTable
  .childPart<RunIds>()
  .withKey('run');

export const jobTable = workflowRunTable.childPart<JobIds>().withKey('job');

export const stepTable = jobTable
  .joinPart<JobIds & { step: string }>()
  .withKey('step');

export const logTable = stepTable
  .joinPart<JobIds & { step: string; log: string }>()
  .withKey('log');

const client = TablePartClient.fromPartsWithBaseTable(
  singleTableDesignDefinition,
  {
    client: dynamoClient,
    logStatements: true,
    tableName: 'singleTableDesignDefinition',
  },
  repositoryTable,
  workflowTable,
  workflowRunTable,
  jobTable,
  stepTable,
  logTable,
);

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
