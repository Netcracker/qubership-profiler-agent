declare module '*.css';
declare module '*.scss';
declare module '*.png';
declare module '*.jpg';
declare module '*.jpeg';
declare module '*.svg' {
    import * as React from 'react';

    export const ReactComponent: React.FunctionComponent<React.ComponentProps<'svg'> & { title?: string }>;
}
declare module '*.svg?file' {}

declare module '*.module.less' {
    const resource: { [key: string]: string };
    export = resource;
}
declare module '*.module.css' {
    const resource: { [key: string]: string };
    export = resource;
}
declare module '*.module.scss' {
    const resource: { [key: string]: string };
    export = resource;
}
