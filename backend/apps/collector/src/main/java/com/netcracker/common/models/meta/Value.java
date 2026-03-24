package com.netcracker.common.models.meta;

import com.netcracker.common.models.StreamType;
import com.netcracker.profiler.sax.io.DataInputStreamEx;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public sealed interface Value permits Value.Str, Value.Clob {

    CharSequence get();

    boolean isEmpty();

    static Str str(String val) {
        return new Str(val);
    }

    static Clob clob(String podReference, StreamType clobType, int fileIndex, int offset) {
        return new Clob(new ClobId(podReference, clobType, fileIndex, offset), new AtomicReference<>(null));
    }

    record Str(String value) implements Value {
        @Override
        public CharSequence get() {
            return value;
        }

        @Override
        public boolean isEmpty() {
            return value == null || value.isEmpty();
        }

        @Override
        public String toString() {
            return value;
        }
    }

    record ClobId(String podReference, StreamType clobType, int fileIndex, int offset) implements Comparable<ClobId> {
        @Override
        public String toString() {
            return String.format("Clob(%s: fileIndex=%d, offset=%d, pod=%s)", clobType, fileIndex, offset, podReference);
        }
        @Override
        public int compareTo(ClobId that) {
            return Objects.compare(this, that,
                    Comparator.comparing(ClobId::clobType)
                            .thenComparing(ClobId::fileIndex)
                            .thenComparing(ClobId::offset)
                            .thenComparing(ClobId::podReference));
        }
    }

    record Clob(ClobId id, AtomicReference<CharSequence> val) implements Value, Comparable<Clob> {
        @Override
        public boolean isEmpty() {
            return val().get() == null;
        }

        @Override
        public CharSequence get() {
            return val.get();
        }

        public void set(CharSequence o) {
            val.set(o);
        }

        public void readFrom(DataInputStreamEx is, int maxLength) throws IOException {
            if (maxLength < 1) return; // empty result on zero character read

            if (is.position() < id.offset()) {
                is.skipBytes(id.offset() - is.position());
            }
            var length = is.readVarInt();
            if (length > maxLength)
                length = maxLength;

            char[] arr = new char[length];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = is.readChar();
            }

            set(new String(arr));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return id.equals(((Clob) o).id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        public int compareTo(Clob o) {
            return id.compareTo(o.id);
        }

        @Override
        public String toString() {
            return id.toString();
        }
    }

}
