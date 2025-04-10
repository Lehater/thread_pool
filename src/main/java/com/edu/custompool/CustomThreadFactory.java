package com.edu.custompool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final AtomicInteger poolCounter = new AtomicInteger(1);

    private final String poolName;
    private final int factoryId;
    private final AtomicInteger threadCreationCounter = new AtomicInteger();
    private final boolean daemon;

    private static final String THREAD_NAME_PREFIX = "worker-thread-";

    public CustomThreadFactory(String poolName, boolean daemon) {
        this.poolName = poolName;
        this.daemon = daemon;
        this.factoryId = poolCounter.getAndIncrement();
    }

    @Override
    public Thread newThread(Runnable task) {
        int threadNumber = threadCreationCounter.incrementAndGet();
        String threadName = poolName + "-" + THREAD_NAME_PREFIX + threadNumber;

        System.out.println("[ThreadFactory] Creating new thread: " + threadName);

        Thread workerThread = new Thread(task, threadName);
        workerThread.setDaemon(daemon);
        workerThread.setUncaughtExceptionHandler((t, e) ->
                logger.error("Uncaught exception in thread {}: {}", t.getName(), e.toString(), e)
        );

        return workerThread;
    }
}