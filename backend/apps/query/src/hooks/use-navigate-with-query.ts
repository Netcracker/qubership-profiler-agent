import { useCallback } from 'react';
import { type NavigateFunction, type NavigateOptions, useNavigate, useSearchParams } from 'react-router-dom';

export function useNavigateWithQuery() {
    const [search] = useSearchParams();
    const navigate = useNavigate();

    const navigateWithQuery: NavigateFunction = useCallback(
        (...args) => {
            const [to, opts] = args;
            if (typeof to === 'number') {
                navigate(to);
                return;
            }
            if (typeof to === 'string') {
                navigate(
                    {
                        pathname: to,
                        search: search.toString(),
                    },
                    opts as NavigateOptions
                );
            } else {
                navigate(
                    {
                        ...to,
                        search: search.toString(),
                    },
                    opts as NavigateOptions
                );
            }
        },
        [navigate, search]
    );

    return navigateWithQuery;
}
