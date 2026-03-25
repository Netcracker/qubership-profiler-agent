import { type PropsWithChildren, memo } from 'react';
import classNames from './highlight-cell.module.scss';
import clsx from 'clsx';

type HighlightCellProps = {
    highlight?: boolean;
};

const HighlightCell = memo<PropsWithChildren<HighlightCellProps>>(({ highlight = false, children }) => {
    return <span className={clsx({ [classNames.redCell]: highlight })}>{children}</span>;
});

HighlightCell.displayName = 'HighlightCell';

export default HighlightCell;
