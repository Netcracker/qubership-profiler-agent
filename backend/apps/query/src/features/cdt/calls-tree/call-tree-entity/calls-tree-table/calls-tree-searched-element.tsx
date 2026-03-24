import type { FC } from 'react';
import { highlight } from '@netcracker/cse-ui-components/utils/highlight';
import { useSearchParams } from 'react-router-dom';
import { ESC_CALL_TREE_QUERY_PARAMS } from '@app/constants/query-params';
import { escapeRegExp } from 'lodash';

interface CallsTreeSearchedElementProps {
    text: string;
}

const CallsTreeSearchedElement: FC<CallsTreeSearchedElementProps> = ({ text }) => {
    const [urlParams] = useSearchParams();
    const callsTreeQuery = urlParams.get(ESC_CALL_TREE_QUERY_PARAMS.callsTreeQuery) || '';

    return highlight(text, escapeRegExp(callsTreeQuery));
};

export default CallsTreeSearchedElement;
