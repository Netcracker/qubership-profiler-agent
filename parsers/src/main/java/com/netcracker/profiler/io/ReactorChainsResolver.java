package com.netcracker.profiler.io;

import static com.netcracker.profiler.io.searchconditions.BaseSearchConditions.putMultimap;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.call.CallDataReaderFactory;
import com.netcracker.profiler.io.call.ReactorCallReader;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import javax.servlet.http.HttpServletRequest;

public abstract class ReactorChainsResolver {
    public List<String>[] splitChainIDs(String[] treeIDs) {
        List<String> regularCalls = new ArrayList<>();
        List<String> chainCalls = new ArrayList<>();
        for(String s: treeIDs){
            if(StringUtils.startsWith(s, "chain_")){
                chainCalls.add(s);
            } else {
                regularCalls.add(s);
            }
        }
        return new List[]{regularCalls, chainCalls};
    }

    public Map<String, String> folderIDMapping(HttpServletRequest req) {
        Map<String,String> result = new HashMap<>();
        for(Map.Entry<String, String[]> entry: req.getParameterMap().entrySet()){
            String key = entry.getKey();
            if(! key.startsWith("f[_") || !key.endsWith("]")){
                continue;
            }
            String folderId = StringUtils.substring(key, 3, key.length()-1);
            if(entry.getValue() == null || entry.getValue().length != 1 || StringUtils.isBlank(entry.getValue()[0])){
                throw new RuntimeException("Illegal value in request by key " + key + ": " + ArrayUtils.toString(entry.getValue()));
            }
            result.put(folderId, entry.getValue()[0]);
        }
        return result;
    }

    private static class Chain{
        String chainId;
        String[] callsStreamIndexes;

        private Chain(String chainId, String[] callsStreamIndexes) {
            this.chainId = chainId;
            this.callsStreamIndexes = callsStreamIndexes;
        }
    }

    public List<CallRowid> resolveReactorChains(HttpServletRequest req, List<String> chainCallIDs){
        List<CallRowid> result = new ArrayList<>();


        Map<String, List<Chain>> foldersToChains = groupChainsByFolders(req, chainCallIDs);
        Map<String, String> folderIdMapping = folderIDMapping(req);

        for(Map.Entry<String, List<Chain>> f2c: foldersToChains.entrySet()){
            String folderId = f2c.getKey();
            Collection<Chain> chains = f2c.getValue();
            String folderName = folderIdMapping.get(f2c.getKey());
            Set<String> chainIDs = new HashSet<>();
            Set<String> toScan = new HashSet<>();
            for(Chain ch: chains) {
                chainIDs.add(ch.chainId);
                toScan.addAll(Arrays.asList(ch.callsStreamIndexes));
            }
            for(String i: toScan){
                int sequenceId = Integer.parseInt(i);
                scanReactorCallsFile(result, folderName, folderId, sequenceId, chainIDs);
            }
        }
        return result;
    }

    protected abstract DataInputStreamEx openReactorCallsStream(String folderName, int sequence) throws IOException;

    private void scanReactorCallsFile(List<CallRowid> result, String folderName, String folderId, int sequence, Set<String> searchIDs){
        Call tmp = new Call();
        try(DataInputStreamEx in = openReactorCallsStream(folderName, sequence)){
            if(in == null){
                return;
            }
            int fileFormat = in.readVarInt();
            ReactorCallReader reader = CallDataReaderFactory.createReactorReader(fileFormat);
            while(true) {
                //since it is filled only when call actually has a chain id
                tmp.reactorChainId = null;
                reader.read(tmp, in);
                if (searchIDs.contains(tmp.reactorChainId)) {
                    result.add(new CallRowid(
                            folderName, Integer.parseInt(folderId),
                            tmp.traceFileIndex,
                            tmp.bufferOffset,
                            tmp.recordIndex,
                            tmp.reactorFileIndex,
                            tmp.reactorBufferOffset
                    ));
                }
            }
        } catch(EOFException ignored) {
            //this is how reading of calls normally ends
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, List<Chain>> groupChainsByFolders(HttpServletRequest req, List<String> chainCallIDs){
        Map<String, List<Chain>> result = new HashMap<>();
        for(String chainIdStr: chainCallIDs){
            String[] split = chainIdStr.split("_");
            if(!StringUtils.equals(split[0], "chain")){
                throw new RuntimeException("chain id should start with chain_");
            }
            String folderId = split[1];
            String chainId = split[2];
            String[] callsStreamIndexes = new String[split.length-3];
            System.arraycopy(split, 3, callsStreamIndexes, 0, split.length-3);
            putMultimap(result, folderId, new Chain(chainId, callsStreamIndexes));
        }
        return result;
    }
}
