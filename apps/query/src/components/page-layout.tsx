import AppHeader, { type AppHeaderProps } from '@app/components/app-header/app-header';
import ConfirmMountPoint from '@app/components/confirm';
import { type FC } from 'react';
import { Outlet } from 'react-router-dom';

type PageLayoutProps = AppHeaderProps

export const PageLayout: FC<PageLayoutProps> = ({version}) => (
    <>
        <ConfirmMountPoint />
        <main className="app">
            <AppHeader version={version}/>
            <div className="app__content">
                <Outlet />
            </div>
        </main>
    </>
);
PageLayout.displayName = 'PageLayout';
