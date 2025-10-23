import cn from 'clsx';
import { type CSSProperties, type FC, memo } from 'react';
import styles from './loading.page.module.scss';
import { UxLoader } from '@netcracker/ux-react/loader/loader.component';

interface LoadingPageProps {
    text?: string;

    className?: string;

    style?: CSSProperties;
}

const LoadingPage: FC<LoadingPageProps> = ({ text, className, style }) => {
    return (
        <div className={cn(styles.container, className)} style={style}>
            <UxLoader size="large" hint={text} />
        </div>
    );
};

export default memo(LoadingPage);
