import { parseMethod } from './method-info';
import type { TreeModel, TreeNode } from './model';

/** Root-to-node path as Java-style stacktrace text, innermost frame first. */
export function stacktraceText(model: TreeModel, node: TreeNode): string {
  const lines: string[] = [];
  for (let cur: TreeNode | null = node; cur !== null; cur = cur.parent) {
    const info = parseMethod(model.methods[cur.methodIdx] ?? '');
    const site = info.fileName === '' ? '' : ` (${info.fileName}:${info.lineNumber})`;
    lines.push(`  at ${info.signature}${site}`);
  }
  return lines.join('\n');
}
