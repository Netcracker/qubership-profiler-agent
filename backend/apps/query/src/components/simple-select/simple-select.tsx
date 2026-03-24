import { type UxInputWrapper } from '@netcracker/cse-ui-components/utils/ux-input-wrapper';
import { UxSelect } from '@netcracker/ux-react/inputs/select';
import type { UxSelectProps, UxSelectValue } from '@netcracker/ux-react/inputs/select/select.model';
import { type Key, memo, useCallback, useMemo } from 'react';

export type SimpleSelectValueChange = string | number | boolean | Key[];

export interface SimpleSelectProps extends Omit<UxSelectProps, 'value' | 'onChange'> {
    value?: string | Key[];

    onChange?: (v?: SimpleSelectValueChange) => void;
}
const SimpleSelect: UxInputWrapper<SimpleSelectProps> = memo(({ value, onChange, options, ...selectProps }) => {
    const selectValue = useMemo(() => {
        const foundOption = options?.find(it => {
            const option = it as UxSelectValue;
            return option.value === value;
        }) as UxSelectValue;
        if (!foundOption && !Array.isArray(value)) {
            if (value) {
                return { value: value, label: value } as UxSelectValue;
            }
        }
        if (!foundOption && Array.isArray(value)) {
            return value.map(it => ({ value: it, label: it }));
        }

        return foundOption;
    }, [options, value]);

    const handleChange = useCallback(
        (opt: any) => {
            if (Array.isArray(opt)) {
                onChange?.(opt.map(it => it.value));
            } else {
                onChange?.(opt?.value);
            }
        },
        [onChange]
    );

    return <UxSelect value={selectValue} onChange={handleChange} options={options} {...selectProps}></UxSelect>;
}) as UxInputWrapper<SimpleSelectProps>;

SimpleSelect.displayName = 'SimpleSelect';
SimpleSelect.__UX_INPUT = true;

export default SimpleSelect;
