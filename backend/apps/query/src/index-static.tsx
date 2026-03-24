import { createRoot } from 'react-dom/client';
import './index.scss';
import { StrictMode } from 'react';
import StaticApp from './components/app-static';

const root = document.getElementById('root');
if (!root) {
    throw new Error('Root element not found');
}
// Render application in DOM
createRoot(root).render(
    // With a StrictMode your app may log warnings in console about mistakes in your code
    // also you will have `api/executeQuery/rejected` from RTKQ - it's correct behavior, don't pay attention on it.
    // you may also can have warnings about React.createPortal, till you are using 18 react version thats warning also can be ignored.
    <StrictMode>
        <StaticApp />
    </StrictMode>
);
