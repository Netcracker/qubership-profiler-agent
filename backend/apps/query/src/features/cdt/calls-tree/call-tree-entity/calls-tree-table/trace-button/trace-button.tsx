import { usePopupVisibleState } from '@netcracker/cse-ui-components';
import { ReactComponent as TraceIconSvg } from '@netcracker/ux-assets/icons/document/document-outline-16.svg';
import { UxButton, UxIcon, UxPopupNew } from '@netcracker/ux-react';
import { UxTextArea } from '@netcracker/ux-react/inputs/input/textarea/textarea.component';
import { type FC } from 'react';
import classNames from '../../content-controls.module.scss';

interface TraceButtonModel {
    text: string;
}

const TraceButton: FC<TraceButtonModel> = ({ text }) => {
    const [visible, close, open] = usePopupVisibleState();

    return (
        <div className={classNames.toolControls}>
            <UxButton type="light" onClick={open}>
                {<UxIcon style={{ fontSize: 16, color: '#0068FF' }} component={TraceIconSvg} />}
            </UxButton>
            <UxPopupNew
                visible={visible}
                header="StackTrace"
                size="large"
                footer={<UxButton onClick={close}>Close</UxButton>}
                // TODO: replace value with real text
                content={<UxTextArea placeholder="Placeholder" readOnly={true} autoSize value={text} />}
                onOk={close}
                onCancel={close}
                onClose={close}
            />
        </div>
    );
};

export default TraceButton;
