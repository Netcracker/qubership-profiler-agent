import type { ServiceDumpInfo } from '@app/store/cdt-openapi';
import { UxButton, UxDropdown, UxIcon, UxMenu } from '@netcracker/ux-react';
import { memo } from 'react';
import { ReactComponent as CloudDownload16Icon } from '@netcracker/ux-assets/icons/cloud-download/cloud-download-16.svg';

const DumpsDownloadOpts = memo<{ opts?: ServiceDumpInfo['downloadOptions'] }>(({ opts }) => {
    if (!opts) return null;
    if (opts.length === 0) return 0;
    return (
        <UxDropdown
            overlay={
                <UxMenu>
                    {opts?.map(it => (
                        <a href={it.uri} target="_blank" rel="noreferrer" key={it.typeName}>
                            <UxMenu.Item>{it.typeName}</UxMenu.Item>
                        </a>
                    ))}
                </UxMenu>
            }
        >
            <UxButton
                type="light"
                size="small"
                leftIcon={<UxIcon style={{ fontSize: 16 }} component={CloudDownload16Icon} />}
            />
        </UxDropdown>
    );
});

DumpsDownloadOpts.displayName = 'DumpsDownloadOpts';

export default DumpsDownloadOpts;
