package com.netcracker.persistence;

import com.netcracker.common.models.SuspendRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.common.models.meta.SuspendHickup;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.op.Operation;
import io.quarkus.logging.Log;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public interface PodsMetaPersistence {

    int DICTIONARY_BATCH_LIMIT = 10000;

    List<ParamsModel> getParams(PodIdRestart pod);

    List<DictionaryModel> getDictionary(PodIdRestart pod);

    List<DictionaryModel> getDictionary(PodIdRestart pod, List<Integer> ids);

    SuspendRange getSuspends(PodIdRestart pod, TimeRange time);

    Operation save(ParamsModel toSave);

    Operation save(DictionaryModel toSave);

    Operation save(SuspendHickup toSave);

}
