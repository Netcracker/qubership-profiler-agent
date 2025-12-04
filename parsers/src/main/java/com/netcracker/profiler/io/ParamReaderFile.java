package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.tags.Dictionary;
import com.netcracker.profiler.tags.DictionaryList;
import com.netcracker.profiler.util.StringUtils;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.jspecify.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.zip.ZipException;

public class ParamReaderFile extends ParamReader {
    File root;

    public static Class<? extends ParamReaderFile> getBestImplementation() {
        if (doesClassExist("com.netcracker.profiler.agent.InflightCall_02")) {
            return ParamReaderFromMemory_03.class;
        }
        if (doesClassExist("com.netcracker.profiler.agent.InflightCall_01")) {
            return ParamReaderFromMemory_02.class;
        }
        if (doesClassExist("com.netcracker.profiler.agent.DumperPlugin_03")) {
            return ParamReaderFromMemory_01.class;
        }
        if (doesClassExist("com.netcracker.profiler.agent.DumperPlugin_01") &&
                doesClassExist("com.netcracker.profiler.agent.Configuration_01")) {
            return ParamReaderFromMemory.class;
        }
        return ParamReaderFile.class;
    }

    private static boolean doesClassExist(String className) {
        try {
            Class.forName(className, false, ParamReader.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @AssistedInject
    public ParamReaderFile(@Assisted("root") @Nullable File root) {
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

    public Dictionary fillTags(final BitSet requiredIds, Collection<Throwable> exceptions) {
        ArrayList<String> tags = new ArrayList<>(requiredIds.size());
        try (DataInputStreamEx calls = DataInputStreamEx.openDataInputStream(root, "dictionary", 1)) {
            int pos = 0;
            for (int i = requiredIds.nextSetBit(0); i >= 0; i = requiredIds.nextSetBit(i + 1)) {
                for (; pos < i; pos++) {
                    calls.skipString();
                    tags.add(null);
                }
                tags.add(calls.readString());
                pos++;
            }
        } catch (EOFException ignored) {
        } catch (ZipException e) {
            if(!"invalid stored block lengths".equals(e.getMessage())) {
                exceptions.add(e);
            }
            //it's ok to get ZipException(invalid stored block lengths) when reading current stream
        } catch (IOException e) {
            exceptions.add(e);
        }
        return new DictionaryList(tags);
    }

    @Override
    public Integer[] findTags(Collection<Throwable> exceptions, String... tagNames) {
        Integer[] result = new Integer[tagNames.length];
        Map<String, Integer> tagsMap = new HashMap<>(tagNames.length);
        for(int i=0; i<tagNames.length; i++) {
            tagsMap.put(tagNames[i], i);
        }
        int tagsFound=0;

        try (DataInputStreamEx calls = DataInputStreamEx.openDataInputStream(root, "dictionary", 1)) {
            int pos = 0;
            while(tagsFound < tagNames.length) {
                String tag = calls.readString();
                Integer idx = tagsMap.get(tag);
                if(idx != null) {
                    result[idx] = pos;
                    tagsFound++;
                }
                pos++;
            }
        } catch (EOFException ignored) {
        } catch (ZipException e) {
            if(!"invalid stored block lengths".equals(e.getMessage())) {
                exceptions.add(e);
            }
            //it's ok to get ZipException(invalid stored block lengths) when reading current stream
        } catch (IOException e) {
            exceptions.add(e);
        }
        return result;
    }

    @Override
    public Dictionary fillCallsTags(Collection<Throwable> exceptions) {
        String[] tags = new String[1000];
        try (DataInputStreamEx callsDictIs = DataInputStreamEx.openDataInputStream(root, "callsDictionary", 1)) {
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
        } catch (EOFException ignored) {
        } catch (ZipException e) {
            if(!"invalid stored block lengths".equals(e.getMessage())) {
                exceptions.add(e);
            }
            //it's ok to get ZipException(invalid stored block lengths) when reading current stream
        } catch (IOException e) {
            exceptions.add(e);
        }
        return new DictionaryList(new ArrayList<>(Arrays.asList(tags)));
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
