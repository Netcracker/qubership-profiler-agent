import { UxTooltip, type UxTooltipProps } from '@netcracker/ux-react';
import {
    type AriaAttributes,
    type CSSProperties,
    type ReactNode,
    memo,
    useCallback,
    useMemo,
    useRef,
    useState,
} from 'react';
import classNames from './html-ellipsis.module.scss';
import clsx from 'clsx';

export interface HtmlEllipsisProps extends AriaAttributes {
    text: ReactNode;

    style?: CSSProperties;

    className?: string;

    tooltipProps?: UxTooltipProps;

    lines?: number;
}

const HtmlEllipsis = memo(({ text, tooltipProps, lines = 1, style, className, ...props }: HtmlEllipsisProps) => {
    const parentRef = useRef<HTMLSpanElement>(null);
    const childRef = useRef<HTMLSpanElement>(null);
    const [canBeVisibleTooltip, setCanBeVisibleTooltip] = useState(false);

    const updateVisibleTooltip = useCallback(() => {
        const childBoundingRect = childRef.current?.getBoundingClientRect();
        if (parentRef.current?.clientHeight && childBoundingRect?.height) {
            setCanBeVisibleTooltip(parentRef.current.clientHeight < childBoundingRect.height);
        }
    }, []);

    const _style: CSSProperties | undefined = useMemo(() => {
        if (lines && lines > 1) {
            if (style) {
                return {
                    ...style,
                    WebkitLineClamp: lines,
                };
            }
            return {
                WebkitLineClamp: lines,
            };
        }
    }, [lines, style]);

    return (
        <UxTooltip
            title={text}
            destroyTooltipOnHide
            trigger={canBeVisibleTooltip ? ['hover'] : 'none'}
            {...tooltipProps}
        >
            <span {...props} style={_style} ref={parentRef} className={clsx(classNames.htmlEllipsis, className)}>
                <span ref={childRef} onMouseEnter={updateVisibleTooltip}>
                    {text}
                </span>
            </span>
        </UxTooltip>
    );
});

HtmlEllipsis.displayName = 'HtmlEllipsis';

export default HtmlEllipsis;
