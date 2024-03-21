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
    expect(result.consumedCapacity!.CapacityUnits).toEqual(0.5);
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
});
