import { UxHeader } from '@netcracker/ux-react/header';
import { type FC, memo } from 'react';

export interface AppHeaderProps {
    version ?: string
}

const appTitle = `Cloud Diagnostic Toolset`;
const AppHeader: FC<AppHeaderProps> = ({version}) => {
    return (
        <UxHeader>
            <UxHeader.Row>
                <UxHeader.Logo />
                <UxHeader.Title text={version ? `${appTitle} v.${version}` : appTitle} />
                <UxHeader.Group></UxHeader.Group>
            </UxHeader.Row>
        </UxHeader>
    );
};

export default memo(AppHeader);
