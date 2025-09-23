package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.util.IOHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public abstract class ParamReader {
    Logger logger = LoggerFactory.getLogger(ParamReader.class);

    public abstract Map<String, ParameterInfoDto> fillParamInfo(Collection<Throwable> exceptions, String rootReference);

    public abstract List<String> fillTags(final BitSet requredIds, Collection<Throwable> exceptions);

    public abstract List<String> fillCallsTags(Collection<Throwable> exceptions);

    @SuppressWarnings("unchecked")
    public void readBig(File root, Map<Integer, Map<Integer, String>> result, String fileName, Collection<Throwable> exceptions, int paramsTrimSize) {
        if (result.isEmpty()) return;
        String rootName = root.getAbsolutePath() + File.separatorChar + fileName + File.separatorChar;

        for (Map.Entry<Integer, Map<Integer, String>> idx : result.entrySet()) {
            DataInputStreamEx dedupIs = null;
            String exceptionMessage = null;
            try {
                try {
                    dedupIs = DataInputStreamEx.reopenDataInputStream(dedupIs, root, fileName, idx.getKey());
                } catch (FileNotFoundException e) {
                    dedupIs = null;
                    exceptionMessage = e.toString();
                    exceptions.add(e);
                } catch (IOException e) {
                    exceptions.add(e);
                    exceptionMessage = e.toString() + ", file " + rootName + idx.getKey() + " reached";
                }
                Map<Integer, String> map = idx.getValue();
                Integer[] ids = map.keySet().toArray(new Integer[map.size()]);
                Arrays.sort(ids);

                for (Integer id : ids) {
                    String value = null;
                    if (dedupIs != null) {
                        try {
                            dedupIs.skipBytes(id - dedupIs.position());

                            StringWriter sw = new StringWriter();
                            int realLength = dedupIs.readString(sw, paramsTrimSize);
                            value = sw.toString();
                        } catch (EOFException e) {
                            exceptions.add(e);
                            exceptionMessage = "End of file " + rootName + idx.getKey() + " reached";
                        } catch (IOException e) {
                            exceptions.add(e);
                            exceptionMessage = e.toString() + ", file " + rootName + idx.getKey() + " reached";
                        }
                    }
                    if (value == null) {
                        value = exceptionMessage + ", byte offset in file " + id;
                    }

                    map.put(id, value);
                }
            } finally {
                IOHelper.close(dedupIs);
            }
        }
    }

    /**
     * Returns Object[]{File dumpRoot, List&lt;Call&gt; calls}
     *
     * @return array with two elements: Object[]{File dumpRoot, List&lt;Call&gt; calls}
     */
    public Object[] getInflightCalls() {
        return null;
    }
}
