import { PageLayout } from '@app/components/page-layout';
import LoadingPage from '@app/pages/loading.page';
import ProfilerPage from '@app/pages/profiler.page';
import { useVersionQuery } from '@app/store/endpoints/esc.endpoint';
import { Persistor, store } from '@app/store/store';
import { Suspense, memo, type FC } from 'react';
import { Provider } from 'react-redux';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { PersistGate } from 'redux-persist/integration/react';

const AppRoutes: FC = () => {
    const { data: version } = useVersionQuery();
    return (
        <BrowserRouter>
            <Routes>
                <Route element={<PageLayout version={version} />}>
                    <Route path="*" element={<ProfilerPage />} />
                    {/* <Route path="*" element={<h1>Not Found 🙈</h1>} /> */}
                </Route>
            </Routes>
        </BrowserRouter>
    );
};

const App: FC = () => {
    return (
        <Suspense fallback={<LoadingPage />}>
            <Provider store={store}>
                <PersistGate persistor={Persistor}>
                    <AppRoutes />
                </PersistGate>
            </Provider>
        </Suspense>
    );
};

export default memo(App);
