import sub from 'date-fns/sub';

export type Range = {
    readonly label: string;
    readonly unit: keyof Duration;
    readonly value: number;
};
export const fastRanges: Readonly<Range[]> = [
    {
        label: 'Last 15 min',
        unit: 'minutes',
        value: 15,
    },
    {
        label: 'Last 1 h',
        unit: 'hours',
        value: 1,
    },
    {
        label: 'Last 2 h',
        unit: 'hours',
        value: 2,
    },
    {
        label: 'Last 4 h',
        unit: 'hours',
        value: 4,
    },
];

export const defaultSelectedRange = fastRanges[0]!;
export const defaultRange = {
    dateFrom: sub(new Date(), { [defaultSelectedRange.unit]: defaultSelectedRange.value })
        .getTime()
        .toString(),
    dateTo: new Date().getTime().toString(),
};
