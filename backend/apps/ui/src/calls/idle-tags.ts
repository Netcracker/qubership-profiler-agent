// The "Hide system/proxy" toggle (09 §2.3) hides housekeeping threads that
// dominate page one when idle. The list is ported verbatim from the old UI's
// idleTags (profiler-ui/src/dataFormat.mjs); it filters client-side until a
// backend hide_system flag exists.
export const IDLE_TAGS: ReadonlySet<string> = new Set([
  'com.netcracker.ejb.cluster.messages.MessageThread.run',
  'com.netcracker.ejb.cluster.DatabaseThread.run',
  'com.netcracker.ejb.cluster.NodeManagerThread.run',
  'com.netcracker.ejb.cluster.NotificationThread.run',
  'com.netcracker.ejb.cluster.RecoveryThread.run',
  'com.netcracker.platform.scheduler.impl.ncjobstore.NCJobStore$RecoveryLockManager.run',
  'com.netcracker.mediation.dataflow.impl.util.trigger.socket.SocketListenerThread.run',
  'com.netcracker.mediation.dataflow.impl.util.recovery.RecoveryThread.run',
  'netscape.ldap.LDAPConnThread.run',
  'org.quartz.impl.jdbcjobstore.JobStoreSupport$MisfireHandler.run',
  'org.quartz.impl.jdbcjobstore.JobStoreSupport$ClusterManager.run',
  'org.quartz.core.QuartzSchedulerThread.run',
  'oracle.jms.AQjmsConsumer.receiveFromAQ',
  'org.apache.tools.ant.taskdefs.StreamPumper.run',
  'weblogic.jms.bridge.internal.MessagingBridge.run',
]);

export function isIdleMethod(method: string): boolean {
  return IDLE_TAGS.has(method);
}
