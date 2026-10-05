package com.netcracker.profiler.test.rules;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.configuration.Rule;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Collections;

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

    private static final String BASIC_CONSUME = "basicConsume(Ljava/lang/String;)Ljava/lang/String;";
    private static final String BASIC_ACK = "basicAck(JZ)V";
    private static final String BYTE_ARRAY_PUBLISH =
            "basicPublish(Ljava/lang/String;Ljava/lang/String;ZZLcom/rabbitmq/client/AMQP$BasicProperties;[B)V";
    private static final String BYTE_BUFFER_PUBLISH =
            "basicPublish(Ljava/lang/String;Ljava/lang/String;ZZLcom/rabbitmq/client/AMQP$BasicProperties;" +
                    "Ljava/nio/ByteBuffer;Lcom/rabbitmq/client/WriteListener;)V";

    private static final String BYTE_BUFFER_PUBLISH_SIGNATURE =
            "basicPublish(java.lang.String, java.lang.String, boolean, boolean, " +
                    "com.rabbitmq.client.AMQP$BasicProperties, java.nio.ByteBuffer, com.rabbitmq.client.WriteListener)";

    @Test
    public void ruleWithoutStructureCriteriaMatchesAnyClass() {
        Rule r = new Rule();
        r.addIncludedMethod("basicPublish");
        assertFalse(r.hasClassStructureCriteria(), "hasClassStructureCriteria of a rule that states no criterion");
        assertTrue(r.matchesClassStructure(asList(BYTE_ARRAY_PUBLISH, BYTE_BUFFER_PUBLISH)),
                "matchesClassStructure(byte[] and ByteBuffer publish)");
    }

    @Test
    public void classThatDeclaresAForbiddenMethodIsRefused() {
        Rule r = new Rule();
        r.addForbiddenClassMethod(BYTE_BUFFER_PUBLISH_SIGNATURE);
        assertTrue(r.hasClassStructureCriteria(), "hasClassStructureCriteria of a rule that forbids a method");
        assertFalse(r.matchesClassStructure(asList(BYTE_ARRAY_PUBLISH, BYTE_BUFFER_PUBLISH)),
                "matchesClassStructure(byte[] and ByteBuffer publish)");
        assertTrue(r.matchesClassStructure(singletonList(BYTE_ARRAY_PUBLISH)),
                "matchesClassStructure(byte[] publish alone)");
    }

    @Test
    public void classThatLacksARequiredMethodIsRefused() {
        Rule r = new Rule();
        r.addRequiredClassMethod(BYTE_BUFFER_PUBLISH_SIGNATURE);
        assertTrue(r.matchesClassStructure(asList(BYTE_ARRAY_PUBLISH, BYTE_BUFFER_PUBLISH)),
                "matchesClassStructure(byte[] and ByteBuffer publish)");
        assertFalse(r.matchesClassStructure(singletonList(BYTE_ARRAY_PUBLISH)),
                "matchesClassStructure(byte[] publish alone)");
    }

    @Test
    public void everyRequiredMethodHasToBeDeclared() {
        Rule r = new Rule();
        r.addRequiredClassMethod("basicPublish");
        r.addRequiredClassMethod("basicConsume");
        assertFalse(r.matchesClassStructure(singletonList(BYTE_ARRAY_PUBLISH)),
                "matchesClassStructure(byte[] publish alone)");
        assertTrue(r.matchesClassStructure(asList(BYTE_ARRAY_PUBLISH, BASIC_CONSUME)),
                "matchesClassStructure(byte[] publish and basicConsume)");
    }

    @Test
    public void oneForbiddenMethodOutOfSeveralIsEnoughToRefuse() {
        Rule r = new Rule();
        r.addForbiddenClassMethod("basicNack");
        r.addForbiddenClassMethod("basicPublish");
        assertFalse(r.matchesClassStructure(singletonList(BYTE_ARRAY_PUBLISH)),
                "matchesClassStructure(byte[] publish alone)");
        assertTrue(r.matchesClassStructure(singletonList(BASIC_ACK)), "matchesClassStructure(basicAck alone)");
    }

    /**
     * The two criteria are independent conjuncts, so a class satisfying one and tripping the other
     * is refused. This is the shape {@code <rule>} admits and the shipped configuration does not use.
     */
    @Test
    public void aClassIsRefusedWhenItTripsTheForbiddenCriterionOfARuleWhoseRequiredOneItSatisfies() {
        Rule r = new Rule();
        r.addRequiredClassMethod("basicPublish");
        r.addForbiddenClassMethod(BYTE_BUFFER_PUBLISH_SIGNATURE);
        assertFalse(r.matchesClassStructure(asList(BYTE_ARRAY_PUBLISH, BYTE_BUFFER_PUBLISH)),
                "matchesClassStructure(byte[] and ByteBuffer publish)");
        assertTrue(r.matchesClassStructure(singletonList(BYTE_ARRAY_PUBLISH)),
                "matchesClassStructure(byte[] publish alone)");
    }

    /**
     * An interface with no methods of its own reaches the filter as an empty list, which no required
     * pattern can match and no forbidden one can trip.
     */
    @Test
    public void aClassThatDeclaresNoMethodSatisfiesOnlyTheForbiddenCriterion() {
        Rule required = new Rule();
        required.addRequiredClassMethod("basicPublish");
        assertFalse(required.matchesClassStructure(Collections.<String>emptyList()),
                "matchesClassStructure(no declared method) of a rule that requires basicPublish");

        Rule forbidden = new Rule();
        forbidden.addForbiddenClassMethod("basicPublish");
        assertTrue(forbidden.matchesClassStructure(Collections.<String>emptyList()),
                "matchesClassStructure(no declared method) of a rule that forbids basicPublish");
    }

    /**
     * {@code ConfigurationReloader} retransforms a class only where the new rules differ from the
     * loaded ones, so a criterion left out of {@link Rule#equals} survives a reload with no effect.
     */
    @Test
    public void twoRulesDifferingOnlyInARequiredMethodAreNotEqual() {
        Rule withCriterion = publishRule();
        withCriterion.addRequiredClassMethod("basicConsume");
        assertNotEquals(publishRule(), withCriterion,
                "a basicPublish rule against the same rule with an if-class-declares criterion");
    }

    @Test
    public void twoRulesDifferingOnlyInAForbiddenMethodAreNotEqual() {
        Rule withCriterion = publishRule();
        withCriterion.addForbiddenClassMethod(BYTE_BUFFER_PUBLISH_SIGNATURE);
        assertNotEquals(publishRule(), withCriterion,
                "a basicPublish rule against the same rule with an if-class-does-not-declare criterion");
    }

    private static Rule publishRule() {
        Rule r = new Rule();
        r.addClass("com.rabbitmq.client.impl.ChannelN");
        r.addIncludedMethod("basicPublish");
        return r;
    }
}
