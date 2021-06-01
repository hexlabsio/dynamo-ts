import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { query } from "./client";

declare module 'aws-sdk/clients/dynamodb' {
    interface DocumentClient {
        queryExtra: typeof query;
    }
}

DocumentClient.prototype.queryExtra = query;

