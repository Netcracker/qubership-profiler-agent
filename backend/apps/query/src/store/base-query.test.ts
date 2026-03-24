import { getAccessToken } from '@app/store/base-query';
import { getAuthHeader } from '@app/store/base-query';

describe('getAccessToken', () => {
    const token = {
        accessToken: null,
        access_token: 54321,
    };

    it('should return valid result for null token', () => {
        const result = getAccessToken();

        expect(result).toMatchInlineSnapshot(`null`);
    });

    it('should return valid result for empty(null) accessToken', () => {
        sessionStorage.setItem('token', JSON.stringify(token));

        const result = getAccessToken();

        expect(result).toMatchInlineSnapshot(`54321`);
    });

    it('should return valid result for not empty accessToken', () => {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        token.accessToken = 12345;

        sessionStorage.setItem('token', JSON.stringify(token));

        const result = getAccessToken();

        expect(result).toMatchInlineSnapshot(`12345`);
    });
});

describe('getAuthHeader', () => {
    const token = {
        accessToken: null,
        access_token: 54321,
        tokenType: null,
        token_type: 'token_',
    };

    it('should return valid result for empty token', () => {
        const result = getAuthHeader();

        expect(result).toMatchInlineSnapshot(`""`);
    });

    it('should return valid result for empty tokenType and empty accessToken', () => {
        sessionStorage.setItem('token', JSON.stringify(token));

        const result = getAuthHeader();

        expect(result).toMatchInlineSnapshot(`"token_ 54321"`);
    });

    it('should return valid result for not empty tokenType and not empty accessToken', () => {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        token.accessToken = 12345;

        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        token.tokenType = 'token';

        sessionStorage.setItem('token', JSON.stringify(token));

        const result = getAuthHeader();

        expect(result).toMatchInlineSnapshot(`"token 12345"`);
    });
});
