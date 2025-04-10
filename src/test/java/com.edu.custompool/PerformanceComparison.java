package com.edu.custompool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PerformanceComparison {

    public static void main(String[] args) {
        final int numberOfTasks = 1000;
        final int taskDuration = 100;
        final int queueSize = 1000;
        final int corePoolSize = 2;
        final int maxPoolSize = 4;
        final int keepAliveTime = 5;

        // Создаем экземпляр кастомного пула с настройками: core=2, max=4, keepAlive=5с, очередь размером 50, minSpareThreads=1.
        CustomExecutor customExecutor = new CustomThreadPool(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                queueSize,
                1,
                "CustomPool"
        );

        // Стандартный пул из ThreadPoolExecutor с аналогичными параметрами.
        ThreadPoolExecutor standardExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new CustomThreadFactory("StandardPool", true)
        );

        List<Future<Boolean>> customFutures = new ArrayList<>();
        List<Future<Boolean>> standardFutures = new ArrayList<>();

        ////////////////////////////////////////////////////////////////////////////////////
        // Тест для кастомного пула.
        System.out.println("Starting performance test for CustomThreadPool...");
        long startTimeCustom = System.currentTimeMillis();
        for (int i = 0; i < numberOfTasks; i++) {
            Future<Boolean> future = customExecutor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    // Имитируем нагрузку
                    Thread.sleep(taskDuration);
                    return true;
                }

                @Override
                public String toString() {
                    return "CustomTask";
                }
            });
            customFutures.add(future);
        }

        //////////////////////////////////////////
        // Ожидаем выполнение всех задач в кастомном пуле.
        for (Future<Boolean> future : customFutures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endTimeCustom = System.currentTimeMillis();
        long durationCustom = endTimeCustom - startTimeCustom;
        System.out.println("CustomThreadPool processing time for "
                + numberOfTasks + " tasks: " + durationCustom + " ms");

        // Корректно завершаем работу кастомного пула.
        customExecutor.shutdown();

        ////////////////////////////////////////////////////////////////////////////////////
        // Тест для стандартного пула.
        System.out.println("\nStarting performance test for ThreadPoolExecutor...");
        long startTimeStandard = System.currentTimeMillis();
        for (int i = 0; i < numberOfTasks; i++) {
            Future<Boolean> future = standardExecutor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Thread.sleep(taskDuration);
                    return true;
                }

                @Override
                public String toString() {
                    return "StandardTask";
                }
            });
            standardFutures.add(future);
        }

        //////////////////////////////////////////
        // Ожидаем выполнение всех задач в стандартном пуле.
        for (Future<Boolean> future : standardFutures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endTimeStandard = System.currentTimeMillis();
        long durationStandard = endTimeStandard - startTimeStandard;
        System.out.println("Standard ThreadPoolExecutor processing time for "
                + numberOfTasks + " tasks: " + durationStandard + " ms");

        // Завершаем стандартный пул корректно.
        standardExecutor.shutdown();
        try {
            if (!standardExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                standardExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            standardExecutor.shutdownNow();
        }
    }
}
