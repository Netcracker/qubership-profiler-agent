package com.netcracker.profiler.agent;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.util.regex.Pattern;

import javax.transaction.xa.Xid;

public class CallInfo {
    public boolean corrupted;
    private String remoteAddress;
    private String requestURL;
    private String ncUser;
    private String endToEndId;
    private String traceId;
    public long transactions;
    public int logWritten; // number of characters written to logfile
    public int logGenerated; // number of characters passed to .log calls
    public long cpuTime;
    public long waitTime; // Object.wait, synchronized and similar methods
    public int queueWaitDuration;
    public long memoryUsed;
    public long fileRead, fileWritten;
    public long netRead, netWritten;
    public int finishTime; // time when the call is finished
    public transient volatile CallInfo next;
    public final boolean isFirstInThread;
    private transient MediationCallInfo mediationInfo;
    public byte isPersist;
    public int additionalReportedTime = 0; //number of milliseconds to add to call duration in the report to account for reactive requests such as reactive database queries

    public boolean isCallRed;
    /**
     * Servlet:
     * module: ('G' | 'P') ' ' url
     * action: queryString
     * clientId: userName '@' remoteAddress
     * clientInfo: threadName ':' XID
     * <p>
     * CommonPage:
     * module: ('G' | 'P') ' ' url
     * action: objectId ':' tabName
     * clientId: userName '@' remoteAddress
     * clientInfo: threadName ':' XID
     * <p>
     * Jobs:
     * module: 'Job ' jobName
     * action: jobClass '.' jobMethod | jobJMSTopic | jobURL
     * clientInfo: threadName ':' XID
     * <p>
     * Workflow:
     * module: 'WF' processTemplateId
     * action: processId ':' activityId ':' actionNumber
     * clientInfo: threadName ':' XID
     * <p>
     * Dataflow:
     * module: 'DF' sessionTemplateId
     * action: sessionId
     * clientInfo: threadName ':' XID
     * <p>
     * Orchestrator:
     * module: 'PO ' processName
     * action: taskId ':' taskName
     */

    public static final int MODULE_LENGTH = 48;
    public static final int ACTION_LENGTH = 32;
    public static final int CLIENT_ID_LENGTH = 64;
    public static final int CLIENT_INFO_LENGTH = 64;

    private String module = "";
    private String action = "";
    private String cliendId = "";
    private String clientInfo;


    private transient boolean anyFieldChanged;
    private transient boolean moduleChanged;
    private transient boolean actionChanged;
    private transient boolean cliendIdChanged;
    private transient boolean clientInfoChanged;

    /**
     * This field stores the most recent physical connection that was updated with end-to-end metrics
     */
    private transient WeakReference<Connection> lastConnection;

    /**
     * This field stores the most recent Xid
     */
    private transient WeakReference<Xid> lastXid;

    public CallInfo() {
        isFirstInThread = true;
    }

    public CallInfo(LocalState state) {
        isFirstInThread = state.callInfo == null;
        setClientInfo(state.shortThreadName);
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestUrl) {
        this.requestURL = requestUrl;
    }

    public String getNcUser() {
        return ncUser;
    }

    public void setNcUser(String ncUser) {
        this.ncUser = ncUser;
    }

    /**
     * Checks if the connection matches the stored in lastConnection one
     *
     * @param con connection to check
     * @return true when the connection is up to date, false otherwise
     */
    public boolean checkConnection(Connection con) {
        if (lastConnection != null && con == lastConnection.get()) return true;
        lastConnection = new WeakReference<Connection>(con);
        return false;
    }

    /**
     * Checks if the given xid matches the stored in this callInfo
     *
     * @param xid string value of the xid to check
     * @return true when stored xid is up to date, false otherwise
     */
    public boolean sameXid(Xid xid) {
        if (lastXid != null && lastXid.get() == xid) return true;
        lastXid = new WeakReference<Xid>(xid);
        return false;
    }

    public void addQueueWait(long duration) {
        if (duration <= 0) {
            return;
        }
        int prevQueueWaitDuration = queueWaitDuration;
        long nextQueueWaitDuration = prevQueueWaitDuration + duration;
        // We do not want 64bit for queue wait duration, so we check for overflow here
        // It is unlikely the want will need more than 2^32ms
        if (nextQueueWaitDuration <= Integer.MAX_VALUE) {
            queueWaitDuration = (int) nextQueueWaitDuration;
        }
    }

    public boolean anyFieldChanged() {
        return anyFieldChanged && !(anyFieldChanged = false);
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        module = StringUtils.cap(module, MODULE_LENGTH);
        anyFieldChanged |= moduleChanged |= StringUtils.stringDiffers(module, this.module);
        this.module = module;
    }

    public boolean moduleChanged() {
        return moduleChanged && !(moduleChanged = false);
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        action = StringUtils.cap(action, ACTION_LENGTH);
        anyFieldChanged |= moduleChanged |= actionChanged |= StringUtils.stringDiffers(action, this.action);
        this.action = action;
    }

    public boolean actionChanged() {
        return actionChanged && !(actionChanged = false);
    }

    public String getCliendId() {
        return cliendId;
    }

    public void setCliendId(String clientId) {
        clientId = StringUtils.cap(clientId, CLIENT_ID_LENGTH);
        anyFieldChanged |= cliendIdChanged |= StringUtils.stringDiffers(clientId, this.cliendId);
        this.cliendId = clientId;
    }

    public boolean clientIdChanged() {
        return cliendIdChanged && !(cliendIdChanged = false);
    }

    public String getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(String clientInfo) {
        clientInfo = StringUtils.cap(clientInfo, CLIENT_INFO_LENGTH);
        anyFieldChanged |= clientInfoChanged |= StringUtils.stringDiffers(clientInfo, this.clientInfo);
        this.clientInfo = clientInfo;
    }

    public boolean clientInfoChanged() {
        return clientInfoChanged && !(clientInfoChanged = false);
    }

    private final static Pattern SPACE = Pattern.compile("\\s+");
    private final static Pattern INVALID_ICOMS_CHARS = Pattern.compile("[()\\[\\]{}$<>:]+");

    public void setEndToEndId(String endToEndId) {
        endToEndId = SPACE.matcher(endToEndId).replaceAll("");
        endToEndId = INVALID_ICOMS_CHARS.matcher(endToEndId).replaceAll("");
        Profiler.event(endToEndId, "end.to.end.id");
        this.endToEndId = endToEndId;
    }

    public String getEndToEndId() {
        return endToEndId;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public MediationCallInfo getMediationInfo() {
        MediationCallInfo mediationInfo = this.mediationInfo;
        if (mediationInfo == null) {
            this.mediationInfo = mediationInfo = new MediationCallInfo();
        }
        return mediationInfo;
    }

    public void clean() {
        // Stops nepotism
        // .next.next forms a linked-list.
        // If CallInfo once promotes to the old-generation, GC would consider next pointer to be a valid reference,
        // thus it will promote other {{CallInfo}} in the chain.
        // Reverting next to null helps GC in such cases
        next = null;
        mediationInfo = null;
    }
}
