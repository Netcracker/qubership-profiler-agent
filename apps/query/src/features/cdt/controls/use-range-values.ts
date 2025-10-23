import { defaultRange } from '@app/features/cdt/controls/fast-ranges';
import { ESC_QUERY_PARAMS } from '@app/constants/query-params';
import { useSearchParams } from 'react-router-dom';

export function useRangeValues() {
    const [searchParams] = useSearchParams();
    const from = searchParams.get(ESC_QUERY_PARAMS.dateFrom) ?? defaultRange.dateFrom;
    const to = searchParams.get(ESC_QUERY_PARAMS.dateTo) ?? defaultRange.dateTo;
    return [from, to] as const;
}
