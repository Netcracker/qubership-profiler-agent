package com.netcracker.profiler.io;

import java.util.Iterator;
import java.util.Map;

public class ActivePODStreamReport extends ActivePODReport {
    public String streamName;
    public ActivePODStreamReport(String podName, String streamName) {
        super(podName);
        this.streamName = streamName;
    }

    public void calculateBitrate(){
        if(lastAndSecondLastData.size() >= 2){
            Iterator<Map.Entry<Long, Long>> it = lastAndSecondLastData.entrySet().iterator();
            Map.Entry<Long, Long> secondLast = it.next();
            Map.Entry<Long, Long> last = it.next();
            this.currentBitrate = ((float)(last.getValue() - secondLast.getValue())) / ((float) (last.getKey() - secondLast.getKey()));
        } else {
            this.currentBitrate = 0f;
        }
    }
}
