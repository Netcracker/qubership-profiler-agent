import { ESC_QUERY_PARAMS } from '@app/constants/query-params';
import { useNamespacesTreeData } from '@app/features/cdt/sidebar/use-namespaces-tree-data';
import LoadingPage from '@app/pages/loading.page';
import type { Container } from '@app/store/cdt-openapi';
import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import { contextDataAction, selectTreeState } from '@app/store/slices/context-slices';
import { InfoPage } from '@netcracker/cse-ui-components';
import { UxTree } from '@netcracker/ux-react';
import { UxSearch } from '@netcracker/ux-react/inputs/input/search/search.component';
import { useCallback, useDeferredValue, useMemo, useRef, useState, type Key } from 'react';
import { useSearchParams } from 'react-router-dom';
import classNames from './namespaces-tree.module.scss';
import { useCheckKeys } from './use-checked-keys';

function filterKeysByFilteredData(filteredData: Container[], checkedKeys: Key[]) {
    return checkedKeys.filter(key => {
        const has1Level = filteredData.findIndex(node => node.namespace === key);
        if (has1Level !== -1) {
            return true;
        }
        const has2Level = filteredData.findIndex(
            node => node.services.findIndex(service => service.name === key) !== -1
        );
        return has2Level !== -1;
    });
}

const emptyArray: Key[] = [];

const NamespacesTree = () => {
    const containerRef = useRef<HTMLDivElement>(null);
    const [search, setSearch] = useState<string>('');
    const searchQuery = useDeferredValue(search);
    const { isFetching, rootNodes, treeData, filteredData, error, isError, isSuccess } =
        useNamespacesTreeData(searchQuery);
    //! TODO: filter these Keys by filteredData.
    const { expandedKeys = rootNodes } = useAppSelector(selectTreeState);
    const checkedKeys = useCheckKeys();
    const filteredCheckedKeys = useMemo(() => {
        if (!search) {
            return checkedKeys;
        }
        if (Array.isArray(checkedKeys)) {
            return filterKeysByFilteredData(filteredData, checkedKeys);
        }
        return {
            checked: filterKeysByFilteredData(filteredData, checkedKeys.checked),
            halfChecked: filterKeysByFilteredData(filteredData, checkedKeys.halfChecked),
        };
    }, [checkedKeys, search, filteredData]);
    const filteredExpandedKeys = useMemo(() => {
        if (!search) {
            return expandedKeys;
        }
        return filterKeysByFilteredData(filteredData, expandedKeys);
    }, [expandedKeys, filteredData, search]);
    const dispatch = useAppDispatch();

    const handleExpand = useCallback(
        (expandedKeys: Key[]) => dispatch(contextDataAction.setExpandedKeys(expandedKeys)),
        [dispatch]
    );

    const [urlParams, setUrlParams] = useSearchParams();

    const handleCheck = useCallback(
        (checkedKeys: { checked: Key[]; halfChecked: Key[] } | Key[] | undefined) => {
            if (checkedKeys) {
                if (Array.isArray(checkedKeys)) {
                    if (checkedKeys.length == 0) {
                        if (urlParams.get(ESC_QUERY_PARAMS.services)) {
                            urlParams.delete(ESC_QUERY_PARAMS.services);
                            setUrlParams(urlParams);
                        }
                    } else {
                        setUrlParams(s => {
                            s.set(ESC_QUERY_PARAMS.services, checkedKeys.join(','));
                            return s;
                        });
                    }
                } else {
                    if (checkedKeys?.checked.length == 0) {
                        if (urlParams.get(ESC_QUERY_PARAMS.services)) {
                            urlParams.delete(ESC_QUERY_PARAMS.services);
                            setUrlParams(urlParams);
                        }
                    } else {
                        setUrlParams(s => {
                            s.set(ESC_QUERY_PARAMS.services, checkedKeys?.checked.join(','));
                            return s;
                        });
                    }
                    if (checkedKeys?.halfChecked.length == 0) {
                        if (urlParams.get(ESC_QUERY_PARAMS.halfCheckedKeys)) {
                            urlParams.delete(ESC_QUERY_PARAMS.halfCheckedKeys);
                            setUrlParams(urlParams);
                        }
                    } else {
                        setUrlParams(s => {
                            s.set(ESC_QUERY_PARAMS.halfCheckedKeys, checkedKeys?.halfChecked.join(','));
                            return s;
                        });
                    }
                }
            }
            checkedKeys && dispatch(contextDataAction.toFirstPage());
        },
        [dispatch, setUrlParams, urlParams]
    );

    return (
        <div className={classNames.container} ref={containerRef}>
            <UxSearch
                size="small"
                placeholder="Search"
                value={search}
                onChange={e => {
                    setSearch(e.target.value);
                }}
            />
            {isError && (
                <InfoPage
                    title="Failed to load namespaces"
                    message={'status' in error ? error.status : error.message}
                />
            )}
            {isFetching && <LoadingPage style={{ height: '100%' }} />}
            {!isFetching && (
                <UxTree
                    treeData={treeData}
                    height={(containerRef.current?.clientHeight ?? 500) - 44}
                    checkable
                    checkStrictly
                    selectable={false}
                    blockNode
                    checkedKeys={isSuccess ? filteredCheckedKeys : emptyArray}
                    virtual
                    onCheck={handleCheck}
                    expandedKeys={filteredExpandedKeys}
                    onExpand={handleExpand}
                />
            )}
        </div>
    );
};

export default NamespacesTree;
