import { describe, expect, it } from 'vitest';

import { sqlSignature } from './sql-signature';

// Mirrors backend/libs/calltree/params_test.go TestSQLSignature — the two
// implementations must agree, or per-node grouping (server) and cross-node
// grouping (params-summary.ts) would classify the same SQL differently.
describe('sqlSignature', () => {
  it.each([
    ['SELECT * FROM orders WHERE id = 123', 'S*FoWi='],
    ['SELECT * FROM orders WHERE id = 4', 'S*FoWi='],
    ["WHERE name = 'John''s', age = 7", 'Wn=a='],
    ['UPDATE t1 SET x2 = ?', 'UtSx=?'],
    ['SELECT ab1cd FROM t', 'SaFt'],
    ['', ''],
  ])('signature of %j is %j', (sql, want) => {
    expect(sqlSignature(sql)).toBe(want);
  });
});
