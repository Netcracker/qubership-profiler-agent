import { PageLayout } from '@app/components/page-layout';
import { getStaticVersion } from '@app/components/static-hooks';
import StaticProfilerPage from '@app/pages/static-profiler.page';
import { staticStore } from '@app/store/store-static';
import { memo, type FC } from 'react';
import { Provider } from 'react-redux';
import { BrowserRouter, Route, Routes } from 'react-router-dom';

const StaticApp: FC = () => {
    const version = getStaticVersion();
    return (
        <Provider store={staticStore}>
            <BrowserRouter>
                <Routes>
                    <Route element={<PageLayout version={version} />}>
                        <Route path="*" element={<StaticProfilerPage />} />
                    </Route>
                </Routes>
            </BrowserRouter>
        </Provider>
    );
};

export default memo(StaticApp);
