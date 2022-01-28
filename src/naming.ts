import * as crypto from "crypto";

export function nameFor(name: string): string {
    return crypto.createHash('md5').update(name).digest('hex');
}