import { type FC, type Key, type SyntheticEvent, memo, useCallback } from 'react';
import { Resizable, type ResizableProps } from 'react-resizable';
import classNames from './resizable-title.module.scss';
import clsx from 'clsx';

export type ResizableTitleProps = Omit<ResizableProps, 'height'> & {
    columnKey?: Key;

    resizable?: boolean;
};

const ResizableTitle: FC<ResizableTitleProps> = ({
    onResize,
    width,
    onResizeStop,
    onResizeStart,
    resizable,
    columnKey,
    ...rest
}) => {
    const onResizeClick = useCallback((e: SyntheticEvent) => e.stopPropagation(), []);
    if (resizable) {
        return (
            <Resizable
                height={0}
                width={width ?? 0}
                handle={
                    <span className={clsx(classNames.resizableHandle, 'resizable-handle')} onClick={onResizeClick} />
                }
                onResize={onResize}
                onResizeStop={onResizeStop}
                onResizeStart={onResizeStart}
                minConstraints={[40, 0]}
                draggableOpts={{ enableUserSelectHack: false }}
            >
                <th {...rest} />
            </Resizable>
        );
    }
    return <th {...rest} />;
};
export default memo(ResizableTitle);
