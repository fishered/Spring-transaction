package dev.cluster.tx.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Names a participant action for automatic HTTP registration. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SagaParticipant {
    String value();
}
