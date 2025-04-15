package org.qubership.profiler.client;

import static java.util.zip.Deflater.BEST_SPEED;
import static org.qubership.profiler.cloud.transport.ProtocolConst.*;

import org.qubership.profiler.agent.DumperCollectorClient;
import org.qubership.profiler.agent.DumperRemoteControlledStream;
import org.qubership.profiler.cloud.transport.EndlessSocketInputStream;
import org.qubership.profiler.cloud.transport.FieldIO;
import org.qubership.profiler.cloud.transport.ProfilerProtocolBlacklistedException;
import org.qubership.profiler.cloud.transport.ProfilerProtocolException;

import org.apache.commons.lang.StringUtils;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public class DefaultCollectorClient implements DumperCollectorClient {
    private static final Logger log = LoggerFactory.getLogger(DefaultCollectorClient.class);
    public static final long BLOCKING_WRITE_TIMEOUT = 10L;
    public static final String ENV_NC_DIAGNOSTIC_FOLDER = "NC_DIAGNOSTIC_FOLDER";
    public static final String ENV_KEYSTORE_FILE_PATH = "NC_DIAGNOSTIC_KEYSTORE";
    public static final String  ENV_KEYSTORE_PASSWORD = "NC_DIAGNOSTIC_KEYSTORE_PASSWD";
    public static final String  TLS_KEYSTORE_PATH = "TLS_KEYSTORE_PATH";
    public static final String  TLS_KEYSTORE_PASSWORD = "TLS_KEYSTORE_PWD";

    public static final int NUM_RETRY_ATTEMPTS = 2;
    public static final int PAUSE_BETWEEN_RETRIES_MILLIS = 1000;

    private final String cloudNamespace;
    private final String microserviceName;
    private final String podName;
    private final String host;
    private final int port;
    private final boolean ssl;
    private Socket socket;
    private boolean needsReconnect = true;
    private FieldIO fieldIO;
    private OutputStream out;
    private BufferedInputStream in;
    private InputStream sin;
    private long version;

    //only single dumper thread uses this counter
    private int pendingAcks = 0;

    private final Map<UUID, Process> runningCommands = new HashMap<>();
    private final Map<String, UUID> streamHandles = new HashMap<>();

    public DefaultCollectorClient(String host,
                                  int port,
                                  boolean ssl,
                                  String cloudNamespace,
                                  String microserviceName,
                                  String podName) {
        this.host = host;
        this.port = port;
        this.ssl = ssl;
        this.cloudNamespace = StringUtils.isEmpty(cloudNamespace) ? "unknown" : cloudNamespace;
        this.microserviceName = StringUtils.isEmpty(microserviceName) ? "unknown" : microserviceName;
        this.podName = StringUtils.isEmpty(podName) ? "unknown" : podName;
        this.version = -1L;

        try {
            getFieldIO();
        } catch (IOException e) {
            throw new ProfilerProtocolException(e);
        }

        reportPodName();
    }

    protected void reportPodName() {
        try {
            //this env is set by the bootstrap scripts within pods
            String diagnosticFolder = System.getenv(ENV_NC_DIAGNOSTIC_FOLDER);
            if (StringUtils.isBlank(diagnosticFolder)) {
                return;
            }
            File podNameFile = new File(new File(diagnosticFolder), "pod.name");
            try (Writer fout = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(podNameFile)))) {
                fout.write(podName);
            }
        } catch (IOException e) {
            log.info("Can not create file with pod name {}", podName);
        }
    }

    private FieldIO getFieldIO() throws IOException {
        if (socket != null && (socket.isClosed() || !socket.isConnected() || needsReconnect)) {
            try {
                Socket prev = socket;
                socket = null;
                fieldIO = null;
                prev.close();
            } catch (IOException e) {
                log.error("Failed to close previously unavailable socket", e);
            }
        }
        if (socket == null) {
            socket = openSocket(host, port, ssl);
            out = new BufferedOutputStream(socket.getOutputStream(), DATA_BUFFER_SIZE);
            if (ZIPPING_ENABLED) {
                out = new GZIPOutputStream(out, DATA_BUFFER_SIZE, true) {
                    {
                        def.setLevel(BEST_SPEED);
                    }
                };
            }

            out.flush();    //because input stream on the other side needs to initialize off of non-zero input
            sin = socket.getInputStream();
            in = new BufferedInputStream(new EndlessSocketInputStream(sin), DATA_BUFFER_SIZE);
            fieldIO = new FieldIO(socket, in, out);
            out.write(COMMAND_GET_PROTOCOL_VERSION_V2);
            fieldIO.writeLong(PROTOCOL_VERSION_V3);
            fieldIO.writeString(podName);
            fieldIO.writeString(microserviceName);
            fieldIO.writeString(cloudNamespace);
            out.flush();
            long version = fieldIO.readLong();

            if (version == PROTOCOL_VERSION_V2 || version == PROTOCOL_VERSION_V3) {
                log.debug("Plain socket client connected. Using protocol version {}. ssl: {}", version, ssl);
                needsReconnect = false;
                pendingAcks = 0;
                runningCommands.clear();
                streamHandles.clear();
                this.version = version;
            } else if (version == BLACK_LISTED_RESP) {
                log.debug("Blacklisted Namespace: {}.", cloudNamespace);
                needsReconnect = false;
                pendingAcks = 0;
                runningCommands.clear();
                streamHandles.clear();
                throw new ProfilerProtocolBlacklistedException("Blacklisted Namespace:  " + cloudNamespace);
            } else {
                try {
                    socket.close();
                    socket = null;
                } catch (Exception e) {
                    log.error("Failed to close socket", e);
                }
                throw new ProfilerProtocolException("Protocol version mismatch. Client version is " + PROTOCOL_VERSION + " server version is " + version);
            }
        }
        return fieldIO;
    }

    private SSLContext getSSLContext() throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        SSLContext sslContext = null;
        String keystoreFilePath = System.getenv(ENV_KEYSTORE_FILE_PATH);
        String keystorePassword = System.getenv(ENV_KEYSTORE_PASSWORD);
        if (StringUtils.isBlank(keystoreFilePath)) {
            keystoreFilePath = System.getenv(TLS_KEYSTORE_PATH);
        }
        if (StringUtils.isBlank(keystorePassword)) {
            keystorePassword = System.getenv(TLS_KEYSTORE_PASSWORD);
        }
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream keyStoreStream = new FileInputStream(keystoreFilePath)) {
            keyStore.load(keyStoreStream, keystorePassword.toCharArray());
        }
        sslContext = SSLContextBuilder.create()
                .loadKeyMaterial(keyStore, keystorePassword.toCharArray())
                .loadTrustMaterial(keyStore, null)
                .build();
        return sslContext;
    }

    private Socket initSocketOrSSL(String host, int port, boolean ssl) throws IOException, KeyManagementException, NoSuchAlgorithmException, CertificateException, KeyStoreException, UnrecoverableKeyException {
        if(ssl) {
            SSLContext sslContext = getSSLContext();

            SSLSocketFactory fac = sslContext.getSocketFactory();

            SSLSocket sslSocket = (SSLSocket) fac.createSocket(host, port);

            return sslSocket;
        }
        return new Socket(host, port);
    }

    private Socket openSocket(String host, int port, boolean ssl) throws IOException {
        Socket result;
        try {
            log.debug("Connecting to {}:{}. SSL: {}", host, port, ssl);
            result = initSocketOrSSL(host, port, ssl);
        } catch (java.net.ConnectException e) {
            String newHost = StringUtils.replace(host, "esc-static-service", "esc-collector-service");
            if (newHost.equals(host)) {
                throw new ProfilerProtocolException("Failed to connect to " + host + ":" + port);
            }
            log.warn("Failed to connect to {}:{}. Attempting a fallback to {}. SSL: {}", host, port, newHost, ssl);
            try {
                result = initSocketOrSSL(newHost, port, ssl);
            } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | CertificateException | KeyStoreException
                    | IOException e1) {
                log.warn("Failed to connect to {}:{}, reason: {}", host, port, e1);
                throw new ProfilerProtocolException("Unable to Connect");
            } catch (Exception e1) {
                log.warn("Failed to connect host: reason: {}", e1);
                throw new ProfilerProtocolException("Unable to Connect");
            }
        } catch (Exception e) {
            log.warn("Failed to connect host: reason: {}", e);
            throw new ProfilerProtocolException("Unable to Connect");
        }


        if (result != null){
            result.setSendBufferSize(Short.MAX_VALUE);
            result.setReceiveBufferSize(Short.MAX_VALUE);
            result.setKeepAlive(true);
            result.setSoTimeout(PLAIN_SOCKET_READ_TIMEOUT);
            result.setSendBufferSize(PLAIN_SOCKET_SND_BUFFER_SIZE);
            result.setReceiveBufferSize(PLAIN_SOCKET_RCV_BUFFER_SIZE);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (needsReconnect) {
            log.debug("shutdown requested but collector client needs reconnect. skipping shutdown");
            return;
        }
        try {
            if (out != null) {
                out.write(COMMAND_CLOSE);
                out.flush();
            }
        } finally {
            needsReconnect = true;
            if (socket != null) {
                socket.close();
                socket = null;
                out = null;
            }
        }
    }

    public DumperRemoteControlledStream createRollingChunk(final String streamName, int requestedRollingSequenceId, boolean resetRequired) throws IOException {
        try {
            return attemptCreateRollingChunk(streamName, requestedRollingSequenceId, resetRequired);
        } catch (Exception e) {
            needsReconnect = true;
            throw new ProfilerProtocolException(e);
        }
    }

    private DumperRemoteControlledStream attemptCreateRollingChunk(final String streamName, int requestedRollingSequenceId, boolean resetRequired) throws IOException {
        FieldIO io = getFieldIO();
        //make sure commands to write data are not mixed with commands to open another stream
        validateWriteDataAcks(true);

        io.writeCommand(COMMAND_INIT_STREAM_V2);
        io.writeString(streamName);
        io.writeInt(requestedRollingSequenceId);

        io.writeInt(resetRequired ? 1 : 0);
        out.flush();

        UUID streamHandle = io.readUUID();
        if (streamHandle == null) {
            throw new ProfilerProtocolException("failed to open stream " + streamName);
        }
        streamHandles.put(streamName, streamHandle);
        long rotationPeriod = io.readLong();
        long requiredRotationSize = io.readLong();
        int serverRollingSequenceId = io.readInt();

        return CollectorClientFactory.instance().wrapOutputStream(
                serverRollingSequenceId,
                streamName,
                rotationPeriod,
                requiredRotationSize,
                this
        );
    }

    public void write(byte[] bytes,
                      int offset,
                      int length,
                      String streamName) throws IOException {
        if (needsReconnect) {
            throw new ProfilerProtocolException("Client needs reconnect. can not write");
        }
        UUID handleId = streamHandles.get(streamName);
        if (handleId == null) {
            throw new RuntimeException("Stream " + streamName + " has not been initialized");
        }
        try {
            do {
                int curLength = Math.min(length, DATA_BUFFER_SIZE);
                attemptWrite(bytes, offset, curLength, handleId);
                offset += curLength;
                length -= curLength;
            } while (length > 0);
        } catch (Exception e) {
            needsReconnect = true;
            log.error("Failed sending packet to collector", e);
            throw new ProfilerProtocolException(e);
        }
    }

    private void attemptWrite(byte[] bytes,
                              int offset,
                              int length,
                              UUID handleId) throws IOException {
        //check that previous writes were successful
        validateWriteDataAcks(false);
        getFieldIO().writeCommand(COMMAND_RCV_DATA);
        getFieldIO().writeUUID(handleId);
        getFieldIO().writeField(bytes, offset, length);
        pendingAcks++;
        //never flush synchronously  and never wait for acknowlegement. flush will be initiated by dumper every 5 sec
    }

    /**
     * @param sync - whether to wait for all the acks
     * @return true if no acks are pending
     * @throws IOException
     */
    public boolean validateWriteDataAcks(boolean sync) throws IOException {
        if (sync) {
            out.flush();
        }
        while (pendingAcks > 0 && (sync || in.available() > 0)) {
            validateAckSync();
        }
        return pendingAcks == 0;
    }

    /**
     * @param id      uuid of command
     * @param command heap, td or top
     * @return true if command successfully submitted, false in case of an error
     */
    private boolean dispatchCommand(UUID id, String command) {
        log.info("Executing command {}: {}", id, command);
        try {
            String execCommand = null;
            String diagnosticFolder = System.getenv(ENV_NC_DIAGNOSTIC_FOLDER);
            if (StringUtils.isBlank(diagnosticFolder)) {
                log.warn("Command {} requires presence of ENV variable NC_DIAGNOSTIC_FOLDER", command);
                return false;
            }

            if ("heap".equals(command)) {
                log.info("run heap request");
                execCommand = diagnosticFolder + "/diagtools heap";
            } else if ("td".equals(command)) {
                log.info("run td request");
                execCommand = diagnosticFolder + "/diagtools dump";
            } else if ("top".equals(command)) {
                log.info("run top request");
                execCommand = diagnosticFolder + "/diagtools dump";
            }
            if (execCommand != null) {
                runningCommands.put(id, Runtime.getRuntime().exec(execCommand));
                return true;
            }
        } catch (Throwable t) {
            log.error("Failed to execute command {}: {}", id, command, t);
        }
        return false;
    }

    private void reportCommandResult(UUID commandId, boolean success) throws IOException {
        out.write(COMMAND_REPORT_COMMAND_RESULT);
        fieldIO.writeUUID(commandId);
        out.write(success ? COMMAND_SUCCESS : COMMAND_FAILURE);
    }

    private void dispatchCommands(int numCommands) throws IOException {
        for (; numCommands > 0; numCommands--) {
            UUID id = fieldIO.readUUID();
            String cmd = fieldIO.readString();
            boolean submitted = dispatchCommand(id, cmd);
            if (!submitted) {
                reportCommandResult(id, false);
            }
        }
        for (Iterator<Map.Entry<UUID, Process>> it = runningCommands.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<UUID, Process> cmd = it.next();
            try {
                int exitValue = cmd.getValue().exitValue();
                reportCommandResult(cmd.getKey(), exitValue == 0);
                it.remove();
            } catch (IllegalThreadStateException e) {
                //not exited yet. will check next time
            }
        }
        out.flush();
    }

    private void validateAckSync() throws IOException {
        try {
            int ack = in.read();
            if (ack < 0) {
                throw new ProfilerProtocolException("unexpected EOF from collector");
            }
            byte byteAck = (byte) ack;
            if (byteAck < 0) {
                if (byteAck == ACK_ERROR_MAGIC) {
                    throw new ProfilerProtocolException("Collector can't accept data. stream rotation is requested");
                } else {
                    throw new ProfilerProtocolException("Received invalid ack response " + ack);
                }
            }
            dispatchCommands(byteAck);
            pendingAcks--;
        } catch (SocketTimeoutException e) {
            throw new ProfilerProtocolException("timed out waiting for ack. Number of pending acks " + pendingAcks, e);
        }
    }

    public void requestAckFlush(boolean doFlush) throws IOException {
        getFieldIO().writeCommand(COMMAND_REQUEST_ACK_FLUSH);
        pendingAcks++;
        if (doFlush) {
            out.flush();
        }
    }

    @Override
    public boolean isOnline() {
        return !needsReconnect && out != null && socket != null;
    }

    public void flush() throws IOException {
        if (needsReconnect) {
            throw new ProfilerProtocolException("Client needs reconnect.can not flush");
        }

        try {
            //validateWriteDataAcks will flush out
            requestAckFlush(false);
            validateWriteDataAcks(true);
        } catch (Exception e) {
            needsReconnect = true;
            throw new ProfilerProtocolException(e);
        }
    }

    public String getPodName() {
        return podName;
    }

    public long getVersion() {
        return version;
    }
}
