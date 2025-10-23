import { type CallsTreeInfo } from '@app/store/cdt-openapi';
import { escapeRegExp } from 'lodash';
import prettyMilliseconds from 'pretty-ms';

function ifTextFitsSearch(text: string, search: string) {
    if (!search) {
        return false;
    }
    const regexp = new RegExp(escapeRegExp(search), 'gi');
    return regexp.test(text);
}

function ifRowShouldBeExpanded(search: string) {
    return (node: CallsTreeInfo) => {
        return (
            ifTextFitsSearch(node.info.title, search) ||
            ifTextFitsSearch(prettyMilliseconds(node.time.total), search) ||
            ifTextFitsSearch(node.suspension.total.toString(), search) ||
            ifTextFitsSearch(prettyMilliseconds(node.time.self), search) ||
            ifTextFitsSearch(node.suspension.self.toString(), search) ||
            ifTextFitsSearch(node.invocations.self.toString(), search) ||
            ifTextFitsSearch(node.info.calls.toString(), search)
        );
    };
}

export function getExpandedRowsBySearch(search: string) {
    const getExpandedRows = (callsTreeNode: CallsTreeInfo): string[] => {
        const children = callsTreeNode.children || [];
        const childExpandedRows = children
            .map(child => getExpandedRows(child))
            .reduce((prev, cur) => prev.concat(cur), []);
        return childExpandedRows.length > 0 || children.some(ifRowShouldBeExpanded(search))
            ? [callsTreeNode.id.toString(), ...childExpandedRows]
            : [];
    };
    return (callsTreeNodes: CallsTreeInfo[]) =>
        callsTreeNodes.map(getExpandedRows).reduce((prev, cur) => prev.concat(cur), []);
}
