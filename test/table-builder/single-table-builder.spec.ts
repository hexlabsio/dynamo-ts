import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { TablePartClient, TablePartInfo } from '../../src';

const dynamo = new DynamoDB({
  endpoint: { hostname: 'localhost', port: 5001, protocol: 'http:', path: '/' },
  region: 'local-env',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
});
const dynamoClient = DynamoDBDocument.from(dynamo);

export type RepoIds = {
  account: string;
  repo: string;
};


export type WorkflowIds = RepoIds & { workflow: string };

export type RunIds = WorkflowIds & { run: string };

export type JobIds = RunIds & { job: string };

export const repositoryTable = TablePartInfo.from<
  RepoIds
>().withKeys('account', 'repo');

export const workflowTable = repositoryTable
  .joinPart<WorkflowIds>()
  .withKey('workflow');

export const workflowRunTable = workflowTable
  .childPart<RunIds>()
  .withKey('run');

export const jobTable = workflowRunTable
  .joinPart<JobIds>()
  .withKey('job');

export const stepTable = jobTable
  .joinPart<JobIds & {step: string}>()
  .withKey('step');

const client = TablePartClient.fromParts({client: dynamoClient, logStatements: true, tableName: 'singleTableDesignDefinition'}, repositoryTable, workflowTable, workflowRunTable, jobTable, stepTable);

describe('Single Table Design', () => {

  beforeAll(async () => {
    await client.run.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run1'});
    await client.job.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run1', job: 'job1'});
    await client.job.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run1', job: 'job2'});
    await client.run.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2'});
    await client.job.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job3'});
    await client.job.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4'});
    await client.step.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4', step: 'step 1'});
    await client.step.put({account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4', step: 'step 2'});
  })

  it('should query child joined to child', async () => {
    const result = await client.job.queryWithParents({
    account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2'
    });
    expect(result.member[0]).toEqual({
      item: {account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2'},
      member: [
        {account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job3'},
        {account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4'}
      ]
    })
  })

  it('should query child joined to child two levels', async () => {
    const result = await client.step.queryWithParents({
      account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2'
    });
    expect(result.member[0]).toEqual({
      item: {account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2'},
      member: [
        {item: { account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job3'}, member: []},
        {item: { account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4'}, member: [
            {account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4', step: 'step 1'},
            {account: 'account', repo: 'repo', workflow: 'workflow', run: 'run2', job: 'job4', step: 'step 2'}
          ]}
      ]
    })
  })
})

