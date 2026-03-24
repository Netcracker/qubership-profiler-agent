package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.meta.dict.Parameter;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import io.quarkus.logging.Log;

import java.time.Instant;
import java.util.*;

public record PodMetaData(PodInfo pod,
                          Map<String, Parameter> registeredParams, // TODO: use DictionaryIndex
                          Map<String, Integer> registeredLiterals,
                          Map<Integer, String> idLiterals
) {

    public PodIdRestart podId() {
        return pod.restartId();
    }

    public String namespace() {
        return pod.namespace();
    }

    public String service() {
        return pod.service();
    }

    public String oldPodName() {
        return pod.oldPodName();
    }

    public Instant startTime() {
        return pod.activeSince();
    }

    public Instant lastActive() {
        return pod.lastActive();
    }

    @Override
    public String toString() {
        return pod.oldPodName();
    }

    public boolean isValid() {
        return true; // TODO pod.isValid();
    }

    public int paramsSize() {
        return registeredParams.size();
    }

    public int tagsSize() {
        return registeredLiterals.size();
    }

    public static PodMetaData empty(PodInfo pod) { // without meta
        return new PodMetaData(pod, new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );
    }

    public void enrichDb(List<ParamsModel> parameters, List<DictionaryModel> dictionary) {
        if (!registeredParams.isEmpty()) {
            Log.warnf("[%s] already have meta data (got %d params, already have %d; got %d tags, already have %d)",
                    pod.podName(),
                    parameters.size(), registeredParams.size(),
                    dictionary.size(), registeredLiterals.size());
        }
        dictionary.forEach(s -> {
            putLiteral(s.position(), s.tag());
        });

        parameters.forEach(sp -> {
            if (!putParameter(sp.paramName(), sp.paramIndex(), sp.paramList(), sp.paramOrder(), sp.signature())) {
                Log.debugf("[%s] invalid tag name '%s'", pod.podName(), sp.paramName());
            }
        });
    }

    public void putLiteral(int idx, String s) {
        registeredLiterals.put(s, idx);
        idLiterals.put(idx, s);
    }

    public boolean putParameter(String name, boolean idx, boolean list, int order, String signature) {
        if (registeredLiterals.containsKey(name)) {
            var p = Parameter.of(name, idx, list, order, signature);
            registeredParams.put(name, p);
            return true;
        }
        return false;
    }

    public String getLiteral(int index) {
        return idLiterals.get(index);
    }

    public Parameter getParameter(int id) { // should be added in enrichDb
        var literal = idLiterals.get(id);
        if (literal != null) {
            return registeredParams.get(literal);
        }
        return null;
    }

}