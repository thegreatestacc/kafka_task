package org.sovliv.kafkatask.service;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 03/06/2025
 */

public interface DataBaseWriter {

    boolean isDbOverloaded();
    void shutdownAndFlush();
    boolean isQueueDrained();
}
