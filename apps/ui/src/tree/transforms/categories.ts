import type { CategoryDef, TreeModel, TreeNode } from '../model';

// Setup categories (old Tree__setupBc / Tree_setupBc_updateValue /
// CallTree_refreshCategories). Config lines are `<category> <pattern>`:
// '*' wildcards, '#' comments; a pattern starting with '>' assigns the
// category to the matching node's children rather than the node itself.
// The longest pattern wins; a child assignment overrides the inherited one
// and colours its whole subtree.

export interface CategoryRule {
  category: CategoryDef;
  pattern: RegExp;
  patternLength: number;
  appliesTo: 'node' | 'children';
}

export interface CategoryConfig {
  categories: CategoryDef[];
  rules: CategoryRule[];
}

function wildcardToRegExp(ref: string): RegExp {
  const escaped = ref.split('*').map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  return new RegExp(escaped.join('.*'));
}

/** How many category colors theme.css defines; the palette repeats after that. */
export const CATEGORY_COLORS = 6;

/**
 * The fill of a category's badge. A theme variable, so the same category reads
 * in the light and the dark theme; the old fixed pastel only suited a light page.
 */
export function categoryColor(index: number): string {
  return `var(--pf-color-cat-${(index % CATEGORY_COLORS) + 1})`;
}

/**
 * The fill of a category's flame graph frame: the hue of its badge, stronger,
 * because a frame is the whole picture and a badge sits on a row.
 */
export function categoryBlockColor(category: CategoryDef): string {
  return category.color.replace('--pf-color-cat-', '--pf-color-flame-');
}

function parseCategoryLines(text: string): {
  raw: { name: string; pattern: string; appliesTo: 'node' | 'children' }[];
  invalidLines: number[];
} {
  const raw: { name: string; pattern: string; appliesTo: 'node' | 'children' }[] = [];
  const invalidLines: number[] = [];
  text.split('\n').forEach((lineRaw, i) => {
    const line = lineRaw.trim();
    if (line === '' || line.startsWith('#')) return;
    const m = /(\S+)\s+(.+\S)/.exec(line);
    if (m === null) {
      invalidLines.push(i + 1);
      return;
    }
    const pattern = m[2]!;
    if (pattern.startsWith('>')) raw.push({ name: m[1]!, pattern: pattern.slice(1), appliesTo: 'children' });
    else raw.push({ name: m[1]!, pattern, appliesTo: 'node' });
  });
  return { raw, invalidLines };
}

export function parseCategoryConfig(text: string): CategoryConfig {
  const { raw } = parseCategoryLines(text);
  // Longest pattern first: the most specific rule wins.
  raw.sort((a, b) => b.pattern.length - a.pattern.length);

  const byName = new Map<string, CategoryDef>();
  const categories: CategoryDef[] = [];
  const rules: CategoryRule[] = [];
  for (const r of raw) {
    let def = byName.get(r.name);
    if (def === undefined) {
      def = { name: r.name, color: categoryColor(categories.length) };
      byName.set(r.name, def);
      categories.push(def);
    }
    rules.push({ category: def, pattern: wildcardToRegExp(r.pattern), patternLength: r.pattern.length, appliesTo: r.appliesTo });
  }
  return { categories, rules };
}

/** Line numbers the modal should flag before Apply silently drops them (PR 708 review #14). */
export function invalidCategoryLines(text: string): number[] {
  return parseCategoryLines(text).invalidLines;
}

/**
 * Assigns the effective category to every node: a matching method switches
 * its subtree's category (a '>' rule switches the children only); deeper
 * matches override. With a null config every assignment clears.
 */
export function applyCategories(model: TreeModel, config: CategoryConfig | null): void {
  const ruleFor = new Map<number, CategoryRule | null>();
  const matchRule = (methodIdx: number): CategoryRule | null => {
    let cached = ruleFor.get(methodIdx);
    if (cached !== undefined) return cached;
    cached = null;
    if (config !== null) {
      const method = model.methods[methodIdx] ?? '';
      for (const rule of config.rules) {
        if (rule.pattern.test(method)) {
          cached = rule;
          break;
        }
      }
    }
    ruleFor.set(methodIdx, cached);
    return cached;
  };

  const visit = (node: TreeNode, inherited: CategoryDef | undefined): void => {
    const rule = matchRule(node.methodIdx);
    let own = inherited;
    let forChildren = inherited;
    if (rule !== null) {
      if (rule.appliesTo === 'node') {
        own = rule.category;
        forChildren = rule.category;
      } else {
        forChildren = rule.category;
      }
    }
    if (own === undefined) delete node.category;
    else node.category = own;
    for (const child of node.children) visit(child, forChildren);
  };
  visit(model.root, undefined);
}
