package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.util.IOHelper;

import org.apache.commons.lang.StringUtils;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class ParamReaderFile extends ParamReader {
    File root;

    public ParamReaderFile(File root) {
        this.root = root;
    }

    public Map<String, ParameterInfoDto> fillParamInfo(Collection<Throwable> exceptions, String rootReference) {
        final Map<String, ParameterInfoDto> info = new HashMap<String, ParameterInfoDto>();

        ParamsStreamVisitorImpl visitor = new ParamsStreamVisitorImpl(rootReference);

        try {
            DataInputStreamEx params = openDataInputStreamFromAnyDate("params");
            new ParamsPhraseReader(params, visitor).parsingPhrases(params.available(), true);
        } catch (IOException e) {
        }

        for (ParameterInfoDto parameterInfoDto : visitor.getAndCleanParams()) {
            info.put(parameterInfoDto.name, parameterInfoDto);
        }


        return info;
    }

    public List<String> fillTags(final BitSet requredIds, Collection<Throwable> exceptions) {
        ArrayList<String> tags = new ArrayList<String>(requredIds.size());
        DataInputStreamEx calls = null;
        try {
            calls = DataInputStreamEx.openDataInputStream(root, "dictionary", 1);
            int pos = 0;
            for (int i = requredIds.nextSetBit(0); i >= 0; i = requredIds.nextSetBit(i + 1)) {
                for (; pos < i; pos++) {
                    calls.skipString();
                    tags.add(null);
                }
                tags.add(calls.readString());
                pos++;
            }
        } catch (FileNotFoundException e) {
            exceptions.add(e);
        } catch (EOFException e) {
        } catch (IOException e) {
            exceptions.add(e);
        } finally {
            IOHelper.close(calls);
        }
        return tags;
    }

    @Override
    public List<String> fillCallsTags(Collection<Throwable> exceptions) {
        String[] tags = new String[1000];
        DataInputStreamEx callsDictIs = null;
        try {
            callsDictIs = DataInputStreamEx.openDataInputStream(root, "callsDictionary", 1);
            while(true) {
                int i = callsDictIs.readVarInt();
                String value = callsDictIs.readString();
                if(i >= tags.length) {
                    String[] newTags = new String[(i+1)*2];
                    System.arraycopy(tags, 0, newTags, 0, tags.length);
                    tags = newTags;
                }
                tags[i] = value;
            }
        } catch (FileNotFoundException e) {
            exceptions.add(e);
        } catch (EOFException e) {
        } catch (IOException e) {
            exceptions.add(e);
        } finally {
            IOHelper.close(callsDictIs);
        }
        return new ArrayList<>(Arrays.asList(tags));
    }

    public DataInputStreamEx openDataInputStreamAllSequences(String streamName) throws IOException {
        return DataInputStreamEx.openDataInputStreamAllSequences(root, streamName);
    }

    protected  DataInputStreamEx openDataInputStreamFromAnyDate(String streamName) throws IOException {
        File streamRoot = new File(root, streamName);
        if(streamRoot.exists()){
            return DataInputStreamEx.openDataInputStreamAllSequences(root, streamName);
        }

        //dump/wlsTomMngdD1/2020/09/04/1599179894954/params
        //          ^^ to here              ^^ from here
        File podRoot = root.getParentFile().getParentFile().getParentFile().getParentFile();
        LinkedList<File> toCheck = new LinkedList<>();
        toCheck.add(podRoot);
        while(toCheck.size() > 0){
            File next = toCheck.poll();
            File[] children = next.listFiles();
            if(children == null) {
                continue;
            }
            for(File child: children){
                if(".".equals(child.getName()) || "..".equals(child.getName()))
                    continue;
                if(child.isDirectory()) {
                    toCheck.add(child);
                    continue;
                }
                String name = child.getName();
                if(name.endsWith(".gz")){
                    name = name.substring(0, name.length()-3);
                }
                //found some non-empty params stream
                if(StringUtils.isNumeric(name) && "params".equals(child.getParentFile().getName())){
                    return DataInputStreamEx.openDataInputStreamAllSequences(child.getParentFile().getParentFile(), "params");
                }
            }
        }
        throw new RuntimeException("Failed to find params folder within " + podRoot.getCanonicalPath());
    }

}
