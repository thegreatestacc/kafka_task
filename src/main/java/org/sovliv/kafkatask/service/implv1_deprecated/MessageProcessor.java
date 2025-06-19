package org.sovliv.kafkatask.service.implv1_deprecated;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 03/06/2025
 */

public interface MessageProcessor {

    boolean isDbOverloaded();
    void shutdownAndFlush();
    boolean isQueueDrained();
}
