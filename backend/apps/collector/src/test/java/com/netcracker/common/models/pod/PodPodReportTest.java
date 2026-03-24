package com.netcracker.common.models.pod;

import com.netcracker.common.models.pod.stat.PodDataAccumulated;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.stat.PodReport;
import com.netcracker.common.models.pod.stat.PodRestartStat;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;

@UnitTest
class PodPodReportTest {

    private static Instant T1 = Instant.parse("2023-07-01T09:20:00Z");

    @Test
    void validPodNameParsing() {
        var pod = PodInfo.of("ns","service", "podName", T1, T1, T1);
        PodReport report = new PodReport(pod);
        report.accumulate(stat("2023-07-01T09:28:00Z", 104587));
        report.accumulate(stat("2023-07-01T09:27:00Z", 103046));
        report.accumulate(stat("2023-07-01T09:26:00Z", 101391));
        report.accumulate(stat("2023-07-01T09:25:00Z", 99529));
//        assertEquals(0, report.activeSinceMillis);
    }

    PodRestartStat stat(String time, long val) {
        return new PodRestartStat( PodIdRestart.of("test-service_123432"), Instant.parse(time), acc(val) );
    }

    PodDataAccumulated acc(long d) {
        PodDataAccumulated dat = PodDataAccumulated.empty();
        dat.append(StreamType.CALLS, true, 104587L);
        dat.append(StreamType.CALLS, false, 104587L);
        return dat;
    };

}