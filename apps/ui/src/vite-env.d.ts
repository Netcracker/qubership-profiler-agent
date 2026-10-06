/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Set to '1' to intercept /api/v1 with the MSW mock (see `npm run dev:mock`). */
  readonly VITE_ENABLE_MSW?: string;
}
