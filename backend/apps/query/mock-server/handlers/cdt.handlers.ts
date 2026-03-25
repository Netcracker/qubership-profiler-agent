import { createCallsTreeData } from '@mock-server/data/callstree';
import type { DashboardEntity } from '@app/store/slices/calls-tree-context-slices';
import { createStaticCallsTree } from '@mock-server/data/callstree';
import { containersStub } from '@mock-server/data/containers';
import { createHeapDumps } from '@mock-server/data/heaps';
import { servicesStub } from '@mock-server/data/services';
import { serverBaseUrl } from '@mock-server/helpers/base-url';
import { RestHandler, rest } from 'msw';

export const cdtHandlers: RestHandler[] = [
    rest.get(`${serverBaseUrl()}/cdt/v2/containers`, (req, res, ctx) => {
        return res(ctx.status(200), ctx.json(containersStub), ctx.delay(300));
    }),

    rest.post(`${serverBaseUrl()}/cdt/v2/containers`, (req, res, ctx) => {
        return res(ctx.status(200), ctx.json([containersStub.at(0)]), ctx.delay(300));
    }),

    rest.post(`${serverBaseUrl()}/cdt/v2/services`, (req, res, ctx) => {
        return res(ctx.status(200), ctx.json(servicesStub));
    }),

    rest.post(`${serverBaseUrl()}/cdt/v2/heaps`, (req, res, ctx) => {
        return res(ctx.status(200), ctx.json(createHeapDumps()));
    }),

    rest.post(`${serverBaseUrl()}/cdt/v2/calls/tree`, (req, res, ctx) => {
        return res(ctx.status(200), ctx.json(createCallsTreeData()), ctx.delay(300));
    }),
    rest.post(`${serverBaseUrl()}/cdt/v2/calls/tree/download`, async (req, res, ctx) => {
        const initialState: DashboardEntity[] = await req.json()
        return res(ctx.status(200), ctx.text(createStaticCallsTree(initialState)), ctx.delay(1000));
    }),
];
