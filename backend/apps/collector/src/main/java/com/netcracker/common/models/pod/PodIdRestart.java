package com.netcracker.common.models.pod;

import java.time.Instant;

public record PodIdRestart(PodName pod, Instant restartTime) implements Comparable<PodIdRestart> {

    public boolean isEmpty() {
        return pod.isEmpty() || restartTime == null || restartTime.equals(Instant.EPOCH);
    }

    public String namespace() {
        return pod().namespace();
    }

    public String service() {
        return pod().service();
    }

    public String podName() {
        return pod().podName();
    }

    public String podId() {
        return pod().name();
    }

    public String oldPodName() {
        return pod().podName() + "_" + restartTime.toEpochMilli(); // compatibility with previous version
//        return pod().name() + "_" + restartTime.toEpochMilli(); // compatibility with previous version
//        return pod().name() + "@" + restartTime.toEpochMilli(); // TODO
    }


    @Override
    public String toString() {
        return oldPodName();
    }

    @Override
    public int compareTo(PodIdRestart o) {
        int c = pod.compareTo(o.pod);
        if (c == 0) {
            c = -restartTime.compareTo(o.restartTime); // in reverse: from newest to older
        }
        return c;
    }

    public static PodIdRestart empty() {
        return new PodIdRestart(PodName.empty(), Instant.EPOCH);
    }

    public static boolean isValid(String originalPodName) {
        Parsed parsed = Parsed.fromOriginal(originalPodName);
        return parsed.isValid();
    }

    public static PodIdRestart of(String originalPodName) {
        Parsed parsed = Parsed.fromOriginal(originalPodName);
        var pid = PodName.of("unknown", parsed.service, parsed.pod);
        return new PodIdRestart(pid, parsed.start);
    }

    public static PodIdRestart of(String namespace, String originalPodName) {
        Parsed parsed = Parsed.fromOriginal(originalPodName);
        var pid = PodName.of(namespace, parsed.service, parsed.pod);
        return new PodIdRestart(pid, parsed.start);
    }

    public static PodIdRestart of(String namespace, String service, String originalPodName) {
        Parsed parsed = Parsed.fromOriginal(originalPodName);
        var pid = new PodName(namespace, service, parsed.pod, originalPodName);
        return new PodIdRestart(pid, parsed.start);
    }

    public static PodIdRestart of(String namespace, String service, String podName, String podId, Instant start) {
        var pid = new PodName(namespace, service, podName, podId);
        return new PodIdRestart(pid, start);
    }

    public static PodIdRestart of(PodName pod, Instant restart) {
        var id = pod.podName() + "_" + restart.toEpochMilli();
        return of(pod.name(), pod.service(), pod.podName(), id, restart);
    }

    public static PodIdRestart getPodInfo(String namespace, String service, String podName, long podRestartTime) {
        return new PodIdRestart(new PodName(namespace, service, podName, podName),
                Instant.ofEpochMilli(podRestartTime));
    }


    public static Instant retrievePodTimestamp(String originalPodName) {
        Parsed parsed = Parsed.fromOriginal(originalPodName);
        return parsed.start;
    }

    public record Parsed(String service, String pod, Instant start) {
        public static final Parsed INVALID_POD_NAME = new Parsed("", "", Instant.EPOCH);

        public boolean isValid() {
            return !service.isEmpty() && !pod.isEmpty() && start.isAfter(Instant.EPOCH);
        }

        // format: esc-test-service-58dfcb97-n4f7w_1675853926859
        public static Parsed fromOriginal(String pod) {
            String[] underArr = pod.split("_");
            if (underArr.length != 2) {
                return Parsed.INVALID_POD_NAME;
            }

            Instant time;
            try {
                long n = Long.parseLong(underArr[1]);
                time = Instant.ofEpochMilli(n);
            } catch (NumberFormatException ex) {
                return Parsed.INVALID_POD_NAME;
            }

            String podName = underArr[0];
            String[] hyphenArr = podName.split("-");
            if (hyphenArr.length < 3) {
                return Parsed.INVALID_POD_NAME;
            }
            return new Parsed(stripSuffix(hyphenArr), podName, time);
        }

        private static String stripSuffix(String[] arr) {
            StringBuffer service = new StringBuffer();
            int n;
            for (n = arr.length - 1; n >= arr.length - 2; n--) {
                if (!arr[n].matches("[a-z0-9]+")) break;
            }
            for (int i = 0; i <= n; i++) {
                if (i > 0) service.append("-");
                service.append(arr[i]);
            }
            return service.toString();
        }
    }
}
