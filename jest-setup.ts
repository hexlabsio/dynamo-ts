import * as tables from './test/tables';
import {writeJestDynamoConfig} from "./src/dynamo-jest-setup";

(async () => writeJestDynamoConfig(tables))();