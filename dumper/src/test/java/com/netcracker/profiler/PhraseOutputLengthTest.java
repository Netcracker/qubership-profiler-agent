package com.netcracker.profiler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.netcracker.profiler.cloud.transport.PhraseOutputStream;
import com.netcracker.profiler.cloud.transport.ProfilerProtocolException;
import com.netcracker.profiler.cloud.transport.ProtocolConst;
import com.netcracker.profiler.dump.DataOutputStreamEx;
import com.netcracker.profiler.dump.IDataOutputStreamEx;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PhraseOutputLengthTest {

    public static final int BAD_TAG_LIMIT = 5119; // 5118 - ok, but 5119 fails (MAX_PHRASE_SIZE/2 - 4 bytes for length)

    @Test
    public void writeBadTag() {
        assertThrows(ProfilerProtocolException.class, () -> {
            String word = createTagWithLength(BAD_TAG_LIMIT);

            ByteArrayOutputStream buffer = new ByteArrayOutputStream(100000);
            PhraseOutputStream remote = new PhraseOutputStream(buffer, ProtocolConst.MAX_PHRASE_SIZE, ProtocolConst.DATA_BUFFER_SIZE);
            IDataOutputStreamEx dictOs = new DataOutputStreamEx(remote);

            repeatTags(dictOs, remote, buffer);

            dictOs.write(word);
            System.out.println("TAG   : dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));

            remote.writePhrase();
            System.out.println("PHRASE: dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));

            dictOs.close();
            System.out.println("CLOSE : dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));
        });
    }


    @Test // (enabled = false)
    public void writeTags() throws IOException {
        String word = createTagWithLength(BAD_TAG_LIMIT - 1);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(100000);
        PhraseOutputStream remote = new PhraseOutputStream(buffer, ProtocolConst.MAX_PHRASE_SIZE, ProtocolConst.DATA_BUFFER_SIZE);
        IDataOutputStreamEx dictOs = new DataOutputStreamEx(remote);

        repeatTags(dictOs, remote, buffer);

        dictOs.write(word);
        System.out.println("TAG   : dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));

        remote.writePhrase();
        System.out.println("PHRASE: dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));

        dictOs.close();
        System.out.println("CLOSE : dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));

        String s = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
        System.out.println("string: " + s.length());

        assertEquals(dictOs.size() + 4 * 3, s.length()); // 3*4 bytes of phrases' lengths
    }

    private static String createTagWithLength(int tagSize) {
        StringBuilder word = new StringBuilder();
        for (int i = 0; i < tagSize; i++) {
            word.append("a");
        }
        System.out.println("BAD TAG len: " + word.length() + ", byte size=" + (2 * word.length()));
        return word.toString();
    }

    private static void repeatTags(IDataOutputStreamEx dictOs, PhraseOutputStream remote, ByteArrayOutputStream buffer) throws IOException {
        for (int i = 0; i < 489; i++) { // 490 -- already next phrase
            String s = "tag00ABCDEF00abcdef" + (i % 10); // 20 chars
            dictOs.write(s);
            remote.writePhrase();

        }
        System.out.println("LOOP  : dict size: " + dictOs.size() + " phrase: " + remote.getPhraseLength() + " | out " + buffer.size() + ", not send: " + (dictOs.size() - buffer.size()));
    }
}
