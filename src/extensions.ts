import { DDBClient } from './client';

import { queryFrom } from './builders/queryBuilder';

declare module './client' {
  interface DDBClient {
    queryFrom: typeof queryFrom;
  }
}

DDBClient.prototype.queryFrom = queryFrom;
