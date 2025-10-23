import { UxButton, UxDropdownNew, UxIcon, type UxDropdownNewItem } from '@netcracker/ux-react';
import { ReactComponent as EditIcon } from '@netcracker/ux-assets/icons/edit/edit-outline-16.svg';
import { ReactComponent as DeleteIcon } from '@netcracker/ux-assets/icons/delete/delete-outline-16.svg';
import { ReactComponent as ActionsIcon } from '@netcracker/ux-assets/icons/actions/actions-20.svg';
import type { FC } from 'react';
import { useDisableWidgetFunction } from '../hooks/use-widgets';
import { type Widget } from '@app/store/slices/calls-tree-context-slices';

const DefaultEntityActions: FC<Widget> = ({ i }) => {
    const disableWidget = useDisableWidgetFunction();

    function handleClick(item: UxDropdownNewItem) {
        switch (item.id) {
            case 'edit':
                console.log('Edit choosen: ', i);
                break;
            case 'remove':
                disableWidget(i);
                break;
        }
    }

    return (
        <UxDropdownNew
            items={[
                {
                    id: 'edit',
                    text: 'Edit',
                    leftIcon: <UxIcon style={{ fontSize: 16 }} component={EditIcon} />,
                },
                {
                    id: 'remove',
                    text: 'Remove',
                    className: 'amarant-label',
                    leftIcon: <UxIcon style={{ fontSize: 16 }} component={DeleteIcon} />,
                },
            ]}
            onItemClick={handleClick}
        >
            <UxButton type="light" leftIcon={<UxIcon component={ActionsIcon} />} />
        </UxDropdownNew>
    );
};

export default DefaultEntityActions;
