import { DocumentClient } from "aws-sdk/clients/dynamodb";
import * as client  from "./client";

declare module 'aws-sdk/clients/dynamodb' {
    interface DocumentClient {
        queryExtra: typeof client.query;
    }
}

DocumentClient.prototype.queryExtra = client.query;

