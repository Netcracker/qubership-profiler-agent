import { type DownloadDumpByIdArg, getDownloadDumpUrl } from '@app/store/cdt-openapi';

describe('getDownloadDumpUrl', () => {
    const dump: DownloadDumpByIdArg = {
        podId: '12345',
        dumpType: 'heap',
        dumpId: 'quarkus-3-vertx-685456586c-2h596_1743109309237-heap-1743152230000',
    };

    it('should return valid URL', () => {
        const result = getDownloadDumpUrl(dump);
        expect(result).toMatchInlineSnapshot(`"/cdt/v2/heaps/download/quarkus-3-vertx-685456586c-2h596_1743109309237-heap-1743152230000"`);
    });
});
