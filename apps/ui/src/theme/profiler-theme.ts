import type { ThemeConfig } from 'antd';

import './theme.css';

// The Qubership APIHUB brand palette mapped onto antd's semantic seed tokens.
// Source values come from the shared MUI theme
// (Netcracker/qubership-apihub-ui, packages/shared/src/themes/palette.ts and
// colors.ts); antd derives the rest of each colour ramp from these seeds. Only
// colours are mapped — APIHUB's Inter / 13px typography is left out so the tree
// density and ch-based field widths stay put.
export const profilerTheme: ThemeConfig = {
  // Emit `--ant-color-*` CSS variables the modules read (calls-page.module.css et al.).
  cssVar: {},
  token: {
    colorPrimary: '#0068FF', // palette.primary.main
    colorInfo: '#61AAF2', // palette.information.main
    // antd couples colorLink to colorInfo; pin it back to primary so links keep
    // APIHUB's stronger blue instead of the pale information tint.
    colorLink: '#0068FF',
    colorSuccess: '#00BB5B', // palette.secondary.main
    colorWarning: '#FFB02E', // palette.warning.main
    colorError: '#FF5260', // palette.error.main
    colorTextBase: '#353C4E', // colors.DEFAULT_TEXT_COLOR — tints the whole neutral ramp blue-slate
    colorBgLayout: '#F5F5FA', // palette.background.default
  },
};
