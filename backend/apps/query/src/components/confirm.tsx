import { confirmApiFactory } from '@netcracker/cse-ui-components/utils/confirm';

const { ConfirmMountPoint, confirm, confirmDelete, destroyConfirm, updateConfirm, asyncConfirm } = confirmApiFactory();

export default ConfirmMountPoint;

export { confirm, confirmDelete, destroyConfirm, updateConfirm, asyncConfirm };
