# CDT - Cloud Diagnostic Toolset UI (aka Profiler aka Execution Stastic Collector aka Calls Viewer)

## TODO

- [x] Calls
    - [x] Make Api call only when apply button click
    - [x] Rewrite calls table columns to new API data
    - [x] Calls Request State display
        - [x] Timeout Exception
        - [x] Server Exception
    - [x] Store column widths in `LocalStorage`
    - [x] By default, the system applies “>5sec” option to Calls Overview table.
    - [x] Tooltip for calls query
    - [ ] Do not call API on Bottom Scroll. Maybe notification?
    - [x] Open Several Calls
    - [x] `Open File` button when no calls selected
    - [x] Unit Tests for `createCallUrl`
    - [ ] Virtualization in calls table 🔥
- [ ] Heap Dumps
    - [x] Should fetch only on apply
- [ ] General UI
    - [x] Time Picker `To` should be dependable on `From`
    - [ ] Do not close picker on date selection
    - [x] Store Namespace Sidebar data in URL

## Local Development

### `npm install` - Install dependencies

### `npm run dev` - will start Frontend & mock-server (MSW enabled)

### `npm run test` (or) `npm run test:ci` - to run tests

## Local Development with Mock server

### `npm run dev`

MSW_ENABLED in <https://localhost:8080> and FE on <http://localhost:3030>.

## Local Development with a Real Backend

In the [env](.env) file there is a `API_URL` that leads to the real backend, used for display on the UI part

### `npm start`

<!-- ### Docker compose

Configuration of the compose you can find in [`.env.compose`](./.env.compose).
Before starting compose you need to run `npm run build` -->

## Production Build

### `npm run build`

Build output is [./build](./build) folder

## Branch Requirements

- Feature branch `feature/{TICKED_ID}`
- Bugfix branch `bugfix/{TICKED_ID}`

## Commit message requirements

```
{TICKED_ID} {Summary}
{empty_line opt}
{description opt}
```
