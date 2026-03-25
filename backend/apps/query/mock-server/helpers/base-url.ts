export const serverBaseUrl = () => {
    if (process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'local') {
        return '';
    }
    if (process.env.NODE_ENV === 'test') {
        return 'https://backend/';
    }
};
