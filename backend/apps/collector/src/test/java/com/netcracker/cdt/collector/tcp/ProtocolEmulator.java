package com.netcracker.cdt.collector.tcp;

import com.netcracker.common.models.StreamType;
import com.netcracker.fixtures.tcp.SocketEmulator;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static com.netcracker.common.ProtocolConst.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public record ProtocolEmulator(SocketEmulator socket, ProfilerAgentReader reader, int bufSize) {

    public void initProtocol(String namespace, String service, String podRestart) throws IOException {
        socket.write.Byte(COMMAND_GET_PROTOCOL_VERSION_V2);
        socket.write.Long(123432L); // clientProtocolVersion
        socket.write.String(podRestart);
        socket.write.String(service);
        socket.write.String(namespace);
        reader.firstAction();
        var serverProtocol = socket.read.Long();
        assertEquals(100605L, serverProtocol);
    }

    public UUID initStream(StreamType stream, int seqId) throws IOException {
        socket.write.Byte(COMMAND_INIT_STREAM_V2);
        socket.write.String(stream.getName());
        socket.write.Int(seqId); // requestedRollingSequenceId
        socket.write.Int(0); // resetRequired
        reader.nextAction();

        var handleId = socket.read.UUID();

        var expectedPeriod = 300000L;
        var expectedRotationSize = 2097152;
        switch (stream) {
            case DICTIONARY, PARAMS -> {
                expectedPeriod = 0;
                expectedRotationSize = 0;
            }
        }
        assertEquals(expectedPeriod, socket.read.Long()); // rotationPeriod
        assertEquals(expectedRotationSize, socket.read.Long()); // requiredRotationSize
        assertEquals(seqId, socket.read.Int());

        return handleId;
    }

    public void sendBuffer(UUID handleId, byte[] data) throws IOException {
        int n = data.length;
        for (int i = 0; i < n; i += bufSize) {
            byte[] arr;
            if (i + bufSize < n) {
                arr = Arrays.copyOfRange(data, i, i + bufSize);
            } else {
                arr = Arrays.copyOfRange(data, i, n);
            }
            if (arr.length > 0) {
                sendData(handleId, arr);
            }
        }
    }

    public void sendData(UUID handleId, byte[] bytes) throws IOException {
        Log.tracef("[%s] write %d bytes", handleId, bytes.length);
        socket.write.Byte(COMMAND_RCV_DATA);
        socket.write.UUID(handleId);
        socket.write.Field(bytes, 0, bytes.length);
        reader.nextAction();
        var b = socket.read.Byte();
        if (b != 0) {
            assertEquals(0, b);
        }
        assertEquals(0, b); // acknowledge from server
    }

    public void requestFlush() throws IOException {
        socket.write.Byte(COMMAND_REQUEST_ACK_FLUSH);
        reader.nextAction();
        var b = socket.read.Byte();
        assertEquals(0, b);
    }

    public void close() throws IOException {
        socket.write.Byte(COMMAND_CLOSE);
        reader.nextAction();
        socket.done();
    }
}
