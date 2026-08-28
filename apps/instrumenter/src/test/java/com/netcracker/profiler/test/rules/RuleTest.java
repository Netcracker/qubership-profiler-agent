package com.netcracker.profiler.test.rules;

import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.configuration.Rule;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

public class RuleTest {

    @Test
    public void emptyRuleShouldMatchEveryFile() {
        Rule r = new Rule();
        assertTrue(r.classNameMatches("com/test/Class"));
        assertTrue(r.classNameMatches("org/test/Test"));
    }

    @Test
    public void fullClassName() {
        Rule r = new Rule();
        r.addClass("com.test.Test");
        assertTrue(r.classNameMatches("com/test/Test"));
        assertFalse(r.classNameMatches("org/test/Test"));
        assertFalse(r.classNameMatches("org/test/test/Test"));
    }

    @Test
    public void simpleMethodName() {
        Rule r = new Rule();
        r.addIncludedMethod("hashCode");
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCodes()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hhashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "HashCode()V;", 100, 0, 0));
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode(Ljava/math/BigInteger;)V;", 100, 0, 0));
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode(J)V;", 100, 0, 0));
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode(JILjava/math/BigInteger;)V;", 100, 0, 0));
    }

    @Test
    public void simpleMethodNameWithNoArgs() {
        Rule r = new Rule();
        r.addIncludedMethod("hashCode()");
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCodes()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hhashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "HashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCode(Ljava/math/BigInteger;)V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCode(J)V;", 100, 0, 0));
    }

    @Test
    public void simpleTypes() {
        Rule r = new Rule();
        r.addIncludedMethod("hashCode(BigInteger, int, String)");
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode(Ljava/math/BigInteger;ILjava/math/String;)V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCode(Ljava/math/BigInteger;JLjava/math/String;)V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCodes()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "HashCode()V;", 100, 0, 0));
    }

    @Test
    public void simpleTypesWithEllipsis() {
        Rule r = new Rule();
        r.addIncludedMethod("hashCode(BigInteger, ..., String)");
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode(Ljava/math/BigInteger;ILjava/math/String;)V;", 100, 0, 0));
        assertTrue(r.matches(Modifier.PUBLIC, "hashCode(Ljava/math/BigInteger;JLjava/math/String;)V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCodes()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "HashCode()V;", 100, 0, 0));
    }

    @Test
    void packageProtectedMethods() {
        Rule r = new Rule();
        r.methodModifier("default");
        assertTrue(r.matches(Modifier.TRANSIENT, "hashCode()V;", 100, 0, 0));
        assertFalse(r.matches(Modifier.PUBLIC, "hashCode()V;", 100, 0, 0));
    }

    @Test
    void packageProtectedClasses() {
        Rule r = new Rule();
        r.classModifier("package protected");
        assertTrue(r.matches(Modifier.TRANSIENT, "java/lang/Object", null, null));
        assertFalse(r.matches(Modifier.PUBLIC, "java/lang/Object", null, null));
    }

    @Test
    void minimalMethodLines() {
        Rule r = new Rule();
        r.setMinimumMethodLines(20);
        assertTrue(r.matches(0, "hashCode()V;", 100, 20, 0));
        assertFalse(r.matches(0, "hashCode()V;", 100, 19, 0));
    }

    @Test
    void minimalMethodBackJumps() {
        Rule r = new Rule();
        r.setMinimumMethodBackJumps(2);
        assertTrue(r.matches(0, "hashCode()V;", 100, 0, 2));
        assertFalse(r.matches(0, "hashCode()V;", 100, 0, 1));
    }

    @Test
    void startMatchesArray() {
        Rule r = new Rule();
        r.addIncludedMethod("read(*,int,int)");
        assertTrue(r.matches(0, "read(BII)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([BII)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([B)I;", 100, 0, 1));
        assertFalse(r.matches(0, "read()I;", 100, 0, 1));
    }

    @Test
    void anyMatchesArray() {
        Rule r = new Rule();
        r.addIncludedMethod("read(any,int,int)");
        assertTrue(r.matches(0, "read([BII)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([B)I;", 100, 0, 1));
        assertFalse(r.matches(0, "read()I;", 100, 0, 1));
    }

    @Test
    void regexpMatchesArray() {
        Rule r = new Rule();
        r.addIncludedMethod("read(^\\[B,int,int)");
        assertTrue(r.matches(0, "read([BII)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([B)I;", 100, 0, 1));
        assertFalse(r.matches(0, "read()I;", 100, 0, 1));
    }

    @Test
    void bracketsMatchesArray() {
        Rule r = new Rule();
        r.addIncludedMethod("read(byte [ ],int,int)");
        assertTrue(r.matches(0, "read([BII)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([B)I;", 100, 0, 1));
        assertFalse(r.matches(0, "read()I;", 100, 0, 1));
    }

    @Test
    void starBracketMatchesArray() {
        Rule r = new Rule();
        r.addIncludedMethod("read( * [],int,int)");
        assertTrue(r.matches(0, "read([BII)I;", 100, 0, 2));
        assertTrue(r.matches(0, "read([Ljava/lang/Object;II)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([[BII)I;", 100, 0, 2));
        assertFalse(r.matches(0, "read([B)I;", 100, 0, 1));
        assertFalse(r.matches(0, "read()I;", 100, 0, 1));
    }

    @Test
    void classNotMatchesArray() {
        Rule r = new Rule();
        r.addIncludedMethod("execute(org.postgresql.core.Query, ParameterList, ResultHandler, int, int, int)");
        assertTrue(r.matches(0, "execute(Lorg/postgresql/core/Query;Lorg/postgresql/core/ParameterList;Lorg/postgresql/core/ResultHandler;III)V", 100, 0, 2));
        assertFalse(r.matches(0, "execute([Lorg/postgresql/core/Query;[Lorg/postgresql/core/ParameterList;Lorg/postgresql/core/ResultHandler;III)V", 100, 0, 2));
    }

    @Test
    void methodWithDollar() {
        Rule r = new Rule();
        r.addIncludedMethod("*\\$*");
        assertTrue(r.matches(0, "test$abc()V", 100, 0, 2));
        assertTrue(r.matches(0, "test$()V", 100, 0, 2));
        assertTrue(r.matches(0, "$test()V", 100, 0, 2));
        assertFalse(r.matches(0, "execute([Lorg/postgresql/core/Que$ry;)V", 100, 0, 2));
    }

}
