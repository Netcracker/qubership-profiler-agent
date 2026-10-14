import type { CSSProperties } from 'react';

/**
 * The one height of the calls controls: the filter bar, "Sort by", the duration chips, the switch labels, and "Run
 * query". antd takes it as `controlHeight`; the markup outside antd reads it as `--pf-control-height`.
 */
export const CONTROL_HEIGHT = 32;

/** The antd token override for a ConfigProvider around the controls. */
export const CONTROL_THEME = { token: { controlHeight: CONTROL_HEIGHT } };

/** The CSS variable for the same height, set on the element that wraps the controls. */
export const CONTROL_HEIGHT_STYLE = { '--pf-control-height': `${CONTROL_HEIGHT}px` } as CSSProperties;
