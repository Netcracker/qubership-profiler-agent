/**
 * Makes Partial only specified keys.
 */
export type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>;

/**
 * Makes required only specified keys. Other will be optional or Partial
 */
export type RequiredOnly<T, K extends keyof T> = Partial<Omit<T, K>> & Required<Pick<T, K>>;

/**
 * Makes required only specified keys. Other will be optional or Partial
 */
export type RequiredBy<T, K extends keyof T> = Omit<T, K> & Required<Pick<T, K>>;
