package org.sovliv.kafkatask.utils;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 07/06/2025
 */

public class Constants {
    public static final int WORKER_THREADS = 20;
    public static final int QUEUE_CAPACITY = 200_000;
    public static final int BATCH_SIZE = 5_000;
    public static final int PARALLEL_BATCHES = 5;
    public static final int TPS_LIMIT = 50_000;
    public static final String DB_ERROR = "DB error: ";
    public static final String DATA_TOPIC = "data-topic";

    //producer service constants
    public static final int VIRTUAL_WORKERS = 10;
    public static final String ALL_MESSAGES_SENT = "All messages sent to Kafka.";
    public static final String STARTING_TO_SEND_MESSAGES = "Starting to send %d messages";
    public static final String MAIN_THREAD_INTERRUPTED = "Main thread interrupted.";
    public static final String WAITING_FLUSH = "Waiting for DB flush...";
    public static final String FLUSH_COMPLETE = "DB flush complete.";
    public static final String INTERRUPTED_DURING_SEND = "Interrupted during send: ";
    public static final String SENT_MESSAGES = "Sent messages: ";
    public static final String SENT_FAILED = "Kafka send failed: ";
    public static final String SOME_DATA_VALUE = "value";

    //jdbc service
    public static final String INTERRUPTED_WHILE_PUTTING_TO_QUEUE = "Interrupted while putting to dbQueue: ";

    //producer controller
    public static final String RESPONSE_ENTITY_BODY = "Sent %d messages.";
}
