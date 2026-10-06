package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

public class TypeCodec<T> {
    public static native TypeCodec.PrimitiveBooleanCodec cboolean();

    public static native TypeCodec.PrimitiveByteCodec tinyInt();

    public static native TypeCodec.PrimitiveShortCodec smallInt();

    public static native TypeCodec.PrimitiveIntCodec cint();

    public static native TypeCodec.PrimitiveLongCodec bigint();

    public static native TypeCodec.PrimitiveLongCodec counter();

    public static native TypeCodec.PrimitiveFloatCodec cfloat();

    public static native TypeCodec.PrimitiveDoubleCodec cdouble();

    public static native TypeCodec<BigInteger> varint();

    public static native TypeCodec<BigDecimal> decimal();

    public static native TypeCodec<String> ascii();

    public static native TypeCodec<String> varchar();

    public static native TypeCodec<ByteBuffer> blob();

    public static native TypeCodec<LocalDate> date();

    public static native TypeCodec.PrimitiveLongCodec time();

    public static native TypeCodec<Date> timestamp();

    public static native TypeCodec<UUID> uuid();

    public static native TypeCodec<UUID> timeUUID();

    public static native TypeCodec<InetAddress> inet();

    public static native TypeCodec<Duration> duration();

    public abstract static class PrimitiveBooleanCodec extends TypeCodec<Boolean> {}
    public abstract static class PrimitiveByteCodec extends TypeCodec<Byte> {}
    public abstract static class PrimitiveShortCodec extends TypeCodec<Short> {}
    public abstract static class PrimitiveIntCodec extends TypeCodec<Integer> {}
    public abstract static class PrimitiveLongCodec extends TypeCodec<Long> {}
    public abstract static class PrimitiveFloatCodec extends TypeCodec<Float> {}
    public abstract static class PrimitiveDoubleCodec extends TypeCodec<Double> {}
}
