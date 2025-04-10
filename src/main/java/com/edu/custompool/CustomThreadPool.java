package com.edu.custompool;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

public class CustomThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    private final String poolName;

    private final CustomThreadFactory threadFactory;
    private final List<BlockingQueue<Runnable>> taskQueues;
    private final List<Worker> workers = new ArrayList<>();

    private final ReentrantLock mainLock = new ReentrantLock();
    private final Condition termination = mainLock.newCondition();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public CustomThreadPool(int corePoolSize,
                            int maxPoolSize,
                            long keepAliveTime,
                            TimeUnit timeUnit,
                            int queueSize,
                            int minSpareThreads,
                            String poolName) {
        if (corePoolSize < 0 || maxPoolSize <= 0 || maxPoolSize < corePoolSize)
            throw new IllegalArgumentException("Invalid pool size");
        if (minSpareThreads < 0 || minSpareThreads > maxPoolSize)
            throw new IllegalArgumentException("Invalid minSpareThreads");

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.poolName = poolName;
        this.threadFactory = new CustomThreadFactory(poolName, true);
        this.taskQueues = new ArrayList<>();

        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException();
        if (isShutdown.get()) rejectTask(command);

        int size;
        mainLock.lock();
        try {
            size = workers.size();
            if (size == 0) rejectTask(command);
        } finally {
            mainLock.unlock();
        }

        int index = roundRobinIndex.getAndUpdate(i -> (i + 1) % size);
        BlockingQueue<Runnable> queue = taskQueues.get(index);

        if (!queue.offer(command)) {
            mainLock.lock();
            try {
                if (workers.size() < maxPoolSize) {
                    addWorker();
                    queue = taskQueues.get(workers.size() - 1);
                }
            } finally {
                mainLock.unlock();
            }

            if (!queue.offer(command)) {
                rejectTask(command);
            } else {
                System.out.println("[Pool] Task accepted into queue #" + command.hashCode() + ": " + command);
            }
        } else {
            System.out.println("[Pool] Task accepted into queue #" + command.hashCode() + ": " + command);
        }

        ensureMinSpareThreads();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (isShutdown.get()) throw new RejectedExecutionException("Pool is shutdown");
        FutureTask<T> future = new FutureTask<>(task);
        try {
            execute(future);
        } catch (RejectedExecutionException e) {
            future.cancel(false);
            throw e;
        }
        return future;
    }

    @Override
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            mainLock.lock();
            try {
                if (workers.isEmpty()) termination.signalAll();
            } finally {
                mainLock.unlock();
            }
        }
    }

    @Override
    public void shutdownNow() {
        if (isShutdown.compareAndSet(false, true)) {
            mainLock.lock();
            try {
                for (BlockingQueue<Runnable> queue : taskQueues) queue.clear();
                for (Worker w : workers) w.thread.interrupt();
            } finally {
                mainLock.unlock();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        mainLock.lock();
        try {
            while (!workers.isEmpty()) {
                if (nanos <= 0L) return false;
                nanos = termination.awaitNanos(nanos);
            }
            return true;
        } finally {
            mainLock.unlock();
        }
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public boolean isTerminated() {
        mainLock.lock();
        try {
            return isShutdown.get() && workers.isEmpty();
        } finally {
            mainLock.unlock();
        }
    }

    private void rejectTask(Runnable task) {
        System.out.println("[Rejected] Task " + task + " was rejected due to overload!");
        throw new RejectedExecutionException("Rejected: " + task);
    }

    private void addWorker() {
        mainLock.lock();
        try {
            if (workers.size() >= maxPoolSize) return;
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
            taskQueues.add(queue);
            Worker worker = new Worker(queue);
            Thread thread = threadFactory.newThread(worker);
            worker.thread = thread;
            workers.add(worker);
            thread.start();
        } finally {
            mainLock.unlock();
        }
    }

    private void ensureMinSpareThreads() {
        mainLock.lock();
        try {
            long idleCount = workers.stream().filter(w -> w.idle.get()).count();
            int need = minSpareThreads - (int) idleCount;
            for (int i = 0; i < need && workers.size() < maxPoolSize; i++) {
                addWorker();
            }
        } finally {
            mainLock.unlock();
        }
    }

    private final class Worker implements Runnable {
        final BlockingQueue<Runnable> queue;
        volatile Thread thread;
        final AtomicBoolean idle = new AtomicBoolean(true);

        Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while (!isShutdown.get() || !queue.isEmpty()) {
                    Runnable task = queue.poll(keepAliveTime, timeUnit);
                    if (task == null && workers.size() > corePoolSize) break;
                    if (task != null) {
                        idle.set(false);
                        try {
                            task.run();
                        } finally {
                            idle.set(true);
                        }
                    }
                }
            } catch (InterruptedException ignored) {
            } finally {
                mainLock.lock();
                try {
                    workers.remove(this);
                    if (workers.isEmpty() && isShutdown.get()) termination.signalAll();
                } finally {
                    mainLock.unlock();
                }
            }
        }
    }
}