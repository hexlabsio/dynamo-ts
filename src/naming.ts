import * as crypto from 'crypto';

export class AttributeNamer {
  static nameFor(name: string): string {
    return crypto.createHash('md5').update(name).digest('hex');
  }
  static uniqueName(): string {
    return `${Math.floor(Math.random() * 10000000)}`;
  }
}
