export type ContainersInfoItem = {
    id: number;
    pid: number;
    name: string;
    kind: 'ns' | 'srv' | 'pod';
};
