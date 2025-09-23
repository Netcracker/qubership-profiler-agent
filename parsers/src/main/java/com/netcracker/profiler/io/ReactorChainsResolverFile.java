package com.netcracker.profiler.io;

import static com.netcracker.profiler.util.ProfilerConstants.REACTOR_CALLS_STREAM;

import com.netcracker.profiler.dump.DataInputStreamEx;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

@Component
@Profile("filestorage")
public class ReactorChainsResolverFile extends ReactorChainsResolver {

    @Value("${com.netcracker.profiler.DUMP_ROOT_LOCATION}")
    private File rootFile;

    @Override
    protected DataInputStreamEx openReactorCallsStream(String folderName, int sequence) throws IOException {
        LinkedList<File> toTraverse = new LinkedList<>();
        toTraverse.add(new File(rootFile, folderName));
        while(toTraverse.size() > 0){
            File cur = toTraverse.pop();
            if(!cur.isDirectory() || ".".equals(cur.getName()) || "..".equals(cur.getName())){
                continue;
            }
            File[] children = cur.listFiles();
            if(children == null) {
                continue;
            }
            if(REACTOR_CALLS_STREAM.equals(cur.getName())){
                for(File f: children){
                    if(!f.isFile()){
                        continue;
                    }
                    String name = StringUtils.replace(f.getName(), ".gz", "");
                    if(!StringUtils.isNumeric(name)){
                        continue;
                    }
                    if(!(Integer.parseInt(name) == sequence)){
                        continue;
                    }
                    return DataInputStreamEx.openDataInputStream(f);
                }
            } else {
                toTraverse.addAll(Arrays.asList(children));
            }
        }
        return null;
    }
}
