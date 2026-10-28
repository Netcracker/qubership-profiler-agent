import type { PodSelection } from '../../filter/legacy';
import type { PresetToken } from './filter-bar';

/** The tooltip of an old link's selection. */
export const SELECTION_TITLE =
  'The services and pods of an old link: a call of any of them matches. Filters cannot express this, because every ' +
  'filter must match, so the link keeps it as it was. Remove it to filter by namespace, service, and pod instead.';

/** The token of an old link's selection, which the query applies; none without a selection. */
export function selectionTokens(selection: PodSelection | null, onRemove: () => void): PresetToken[] {
  if (selection === null) return [];
  const value = [...selection.services, ...selection.pods].join(', ');
  return [{ key: 'selection', field: 'service or pod', op: 'in', value, title: SELECTION_TITLE, inQuery: true, onRemove }];
}
