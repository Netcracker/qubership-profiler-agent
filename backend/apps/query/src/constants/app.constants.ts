export const __prod__ = process.env.NODE_ENV === 'production';
export const __dev__ = process.env.NODE_ENV === 'development';
export const __test__ = process.env.NODE_ENV === 'test';

export const API_BASE_URL = __test__ ? 'https://backend/' : '/';
