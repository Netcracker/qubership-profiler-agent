package org.qubership.platform.scheduler.impl.jobs;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.StringUtils;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;

public class AbstractJobImpl {
    protected void dumpContext$profiler(JobExecutionContext ctx) {
        final JobDetail jobDetail = ctx.getJobDetail();
        Profiler.event(jobDetail.getName(), "job.id");

        final JobDataMap map = jobDetail.getJobDataMap();

        CallInfo callInfo = Profiler.getState().callInfo;

        {
            String name = (String) map.get("name");
            Profiler.event(name, "job.name");
            name = "Job " + name;
            callInfo.setModule(name);
        }
        String action = null;
        Profiler.event(map.get("Action Type"), "job.action.type");
        final String jobClass = (String) map.get("Service Name");
        if (jobClass != null) {
            action = jobClass;
            Profiler.event(jobClass, "job.class");
        }
        final String jobMethod = (String) map.get("Method");
        if (jobMethod != null) {
            action = action + '.' + jobMethod;
            Profiler.event(jobMethod, "job.method");
        }
        Profiler.event(map.get("JMS Connection Factory"), "job.jms.connection.factory");
        final String topic = (String) map.get("JMS Topic");
        if (topic != null) {
            action = topic;
            Profiler.event(topic, "job.jms.topic");
        }
        final String url = (String) map.get("URL");
        if (url != null) {
            action = url;
            Profiler.event(url, "job.url");
        }
        if (action != null) {
            action = StringUtils.right(action, CallInfo.ACTION_LENGTH);
            callInfo.setAction(action);
        }
    }
}
