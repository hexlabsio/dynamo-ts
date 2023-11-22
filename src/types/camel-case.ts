type CamelCaseKey<K> = K extends `${infer F}${infer TAIL}`
  ? `${Lowercase<F>}${TAIL}`
  : K;
export type CamelCaseKeys<T> = { [K in keyof T as CamelCaseKey<K>]: T[K] };
