package com.netcracker.profilerTest.testapp;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperConstants;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_07;
import com.netcracker.profiler.agent.LocalState;
import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;

import java.time.Duration;

/**
 * A profiled workload whose method names, parameter keys and parameter values
 * carry adversarial content. It drives the real profiler agent through the
 * programmatic {@link Profiler} API so it can emit arbitrary text on every
 * string surface the backend decodes: method names, parameter keys and
 * parameter values all flow through the shared agent dictionary
 * ({@code MethodDictionary} / {@link ProfilerData#resolveTag(String)}), which
 * the dumper serialises to the {@code dictionary} stream in id order.
 *
 * <p>The workload is split into two independent top-level calls, one per bug,
 * so each backend assertion is unambiguous:
 *
 * <ul>
 *   <li><b>Call A — Bug A (readChar signedness).</b> The agent writes UTF-16
 *       code units (2 bytes each, {@code DataOutputStreamEx.writeChars}). The Go
 *       reader ({@code libs/parser/pipe/pipe_reader.go readChar}) reads a
 *       <em>signed</em> {@code int16}, so any code unit &ge; U+8000 — most
 *       CJK/Hangul and both halves of a non-BMP surrogate pair — decodes to the
 *       wrong rune (U+FFFD). This call registers its dictionary words
 *       <em>before</em> any empty word, so it isolates bug A.</li>
 *   <li><b>Call B — Bug B (empty dictionary word).</b> The agent registers every
 *       phrase, including the empty string. The Go dictionary reader
 *       ({@code libs/parser/pipe/dictionary.go}) skips an empty word
 *       <em>without</em> advancing its id counter, and the collector appends
 *       words by arrival order, so every later id shifts down by one. This call
 *       resolves an empty word first, then registers a plain-ASCII method and
 *       params <em>after</em> it, so all of them resolve to the wrong
 *       (shifted-by-one) dictionary word.</li>
 * </ul>
 *
 * <p>The workload deliberately avoids relying on bytecode instrumentation of its
 * own methods (a Java identifier cannot carry arbitrary Unicode). It opens each
 * synthetic call with {@code LocalState.enter}, attaches parameters with
 * {@code LocalState.event} and closes it with {@code LocalState.exit}; the
 * enclosing {@code main} is left uninstrumented so each synthetic call is a
 * top-level request.
 *
 * <p>The {@code EXPECTED_*} constants are what a correct backend must return
 * byte-exact. The E2E test
 * ({@code backend/libs/tests/smoke_realagent/realagent_test.go}) asserts them
 * and fails today on bugs A and B.
 */
public final class AdversarialMain {

    private AdversarialMain() {
    }

    // --- Adversarial Unicode building blocks (bug A) ---------------------

    /** U+8A9E CJK "語" — a single UTF-16 code unit >= 0x8000. */
    static final String CJK = "語";

    /** U+D55C Hangul "한" — a single UTF-16 code unit >= 0x8000. */
    static final String HANGUL = "한";

    /**
     * U+1F525 fire emoji "🔥" — a non-BMP code point encoded as the surrogate
     * pair U+D83D U+DD25; both halves are >= 0x8000 (surrogate path of bug A).
     */
    static final String EMOJI = "🔥";

    /** The adversarial glyph run used across method name, param key and value. */
    static final String UNICODE = CJK + HANGUL + EMOJI;

    // --- Call A surfaces: exact Unicode the backend must return ----------

    /**
     * Call A's method name. A profiler method name is conventionally
     * {@code "<ret> <fqcn>.<method>(<args>) (<file>) [<jar>]"}; the adversarial
     * glyphs sit inside that shape so the backend's method formatter accepts it.
     */
    static final String EXPECTED_METHOD_A =
            "void com.acme.Svc." + UNICODE + "_handle() (AdversarialMain.java) [test-app.jar]";

    /** Call A param key carrying the adversarial glyphs (bug A on a key). */
    static final String EXPECTED_PARAM_KEY_A = "param." + UNICODE;

    /** Call A param value carrying the adversarial glyphs (bug A on a value). */
    static final String EXPECTED_PARAM_VALUE_A = "value-" + UNICODE + "-tail";

    // --- Call B surfaces: plain ASCII, shifted by the empty word (bug B) --

    /**
     * The empty word registered first, so that Call B's later dictionary entries
     * all shift by one when the Go reader skips it.
     */
    static final String EMPTY_WORD = "";

    /** Call B's method name — plain ASCII, must return byte-exact. */
    static final String EXPECTED_METHOD_B =
            "void com.acme.Svc.plainAsciiHandleB() (AdversarialMain.java) [test-app.jar]";

    /** Call B param keys/values — plain ASCII, must return byte-exact. */
    static final String EXPECTED_PARAM_KEY_B1 = "param.b.alpha";
    static final String EXPECTED_PARAM_VALUE_B1 = "value-b-alpha";
    static final String EXPECTED_PARAM_KEY_B2 = "param.b.beta";
    static final String EXPECTED_PARAM_VALUE_B2 = "value-b-beta";

    // --- Workload ---------------------------------------------------------

    /**
     * Call A: opens one synthetic top-level call whose method name and
     * parameters carry the adversarial Unicode, holds it open long enough to be
     * recorded, then closes it. Registers its dictionary words before any empty
     * word so only bug A can affect it.
     */
    static void recordUnicodeCall() throws InterruptedException {
        int enterTag = ProfilerData.resolveTag(EXPECTED_METHOD_A) | DumperConstants.DATA_ENTER_RECORD;
        LocalState state = Profiler.getState();
        state.enter(enterTag);
        try {
            // Clears MINIMAL_LOGGED_DURATION so the call is recorded; also gives
            // the call a human-visible >1s duration.
            Thread.sleep(1200);
            state.event(EXPECTED_PARAM_VALUE_A,
                    ProfilerData.resolveTag(EXPECTED_PARAM_KEY_A) | DumperConstants.DATA_TAG_RECORD);
        } finally {
            state.exit();
        }
    }

    /**
     * Call B: resolves an empty word first, then opens a plain-ASCII top-level
     * call. Because the Go dictionary reader skips the empty word without
     * advancing its id, every dictionary entry registered here resolves to the
     * wrong (shifted-by-one) word.
     */
    static void recordEmptyWordCall() throws InterruptedException {
        // Register the empty word BEFORE the call's method and params so it sits
        // ahead of them in dictionary id order and shifts them all.
        ProfilerData.resolveTag(EMPTY_WORD);

        int enterTag = ProfilerData.resolveTag(EXPECTED_METHOD_B) | DumperConstants.DATA_ENTER_RECORD;
        LocalState state = Profiler.getState();
        state.enter(enterTag);
        try {
            Thread.sleep(1200);
            state.event(EXPECTED_PARAM_VALUE_B1,
                    ProfilerData.resolveTag(EXPECTED_PARAM_KEY_B1) | DumperConstants.DATA_TAG_RECORD);
            state.event(EXPECTED_PARAM_VALUE_B2,
                    ProfilerData.resolveTag(EXPECTED_PARAM_KEY_B2) | DumperConstants.DATA_TAG_RECORD);
        } finally {
            state.exit();
        }
    }

    /**
     * Flushes the current thread's profiler buffer and waits for the dumper to
     * stream the data to the collector. Mirrors {@code Main.flushProfiler}.
     */
    static void flushProfiler() throws InterruptedException {
        LocalState state = Profiler.getState();
        Profiler.exchangeBuffer(state.buffer);
        DumperPlugin dumper = Bootstrap.getPlugin(DumperPlugin.class);
        Duration timeout = Duration.ofSeconds(15);
        if (dumper instanceof DumperPlugin_07) {
            ((DumperPlugin_07) dumper).gracefulShutdown(timeout.toMillis());
        } else {
            Thread.sleep(timeout.toMillis());
        }
    }

    // Keep main uninstrumented (see config) so each recorded call is top-level.
    public static void main(String[] args) throws Exception {
        System.out.println("[adversarial] EXPECTED_METHOD_A=" + EXPECTED_METHOD_A);
        System.out.println("[adversarial] EXPECTED_PARAM_KEY_A=" + EXPECTED_PARAM_KEY_A);
        System.out.println("[adversarial] EXPECTED_PARAM_VALUE_A=" + EXPECTED_PARAM_VALUE_A);
        System.out.println("[adversarial] EXPECTED_METHOD_B=" + EXPECTED_METHOD_B);

        System.out.println("[adversarial] recording Call A (Unicode, bug A)...");
        recordUnicodeCall();

        System.out.println("[adversarial] recording Call B (empty word shift, bug B)...");
        recordEmptyWordCall();

        System.out.println("[adversarial] flushing profiler...");
        flushProfiler();
        System.out.println("[adversarial] done.");
    }
}
