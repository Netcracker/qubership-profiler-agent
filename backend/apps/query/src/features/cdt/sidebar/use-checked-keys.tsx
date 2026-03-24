import { useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ESC_QUERY_PARAMS } from '@app/constants/query-params';

export function useCheckKeys() {
    const [urlParams] = useSearchParams();
    const checked = urlParams.get(ESC_QUERY_PARAMS.services);
    const halfChecked = urlParams.get(ESC_QUERY_PARAMS.halfCheckedKeys);
    return useMemo(() => {
        const checkedKeys = checked?.split(',') ?? [];
        if (checked) {
            if (!halfChecked) {
                return checkedKeys;
            } else {
                return {
                    checked: checkedKeys,
                    halfChecked: halfChecked?.split(','),
                };
            }
        }
        return [];
    }, [checked, halfChecked]);
}
