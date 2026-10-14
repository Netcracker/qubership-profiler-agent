import { parseMethod } from '../tree/method-info';

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

/**
 * The filter-bar token of "Hide system/proxy" (09 §2.3). The URL keeps the `hide_system=1` flag rather than this
 * condition, so a saved link does not freeze the pattern list (redesign plan §4.2); the plan expands the flag to a
 * `method !~` condition at request time once the backend takes `!~` and /config serves the patterns (§4.6).
 */
export const HIDE_SYSTEM_TOKEN = {
  field: 'method',
  op: '!~',
  value: 'system/proxy',
  title:
    `Hides ${IDLE_TAGS.size} system and proxy methods, such as scheduler and cluster threads. ` +
    'The backend cannot filter by method !~ yet, so this hides the loaded rows in the browser.',
} as const;

export function isIdleMethod(method: string): boolean {
  // `method` is normally a full dictionary word (return type, args, file/line,
  // jar — 08 §7), which never equals the bare `package.Class.method` entries
  // above; parse it down to that shape before matching (the old UI's
  // isTagIdle did the same over its own decoded tag).
  if (IDLE_TAGS.has(method)) return true;
  return IDLE_TAGS.has(parseMethod(method).classMethod);
}
