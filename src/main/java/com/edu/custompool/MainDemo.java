package com.edu.custompool;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class MainDemo {

    public static void main(String[] args) {
        // Создание экземпляра кастомного пула потоков.
        CustomExecutor executor = new CustomThreadPool(
                2,                // corePoolSize
                4,                // maxPoolSize
                5,                // keepAliveTime
                TimeUnit.SECONDS, // единицы измерения keepAliveTime
                5,                // queueSize
                1,                // minSpareThreads (резервных потоков)
                "MyCustomPool"    // имя пула для формирования имен потоков
        );

        // Отправляем несколько задач в пул.
        for (int i = 1; i <= 10; i++) {
            final int taskId = i;
            try {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("[MainDemo] Task " + taskId + " started on " +
                                Thread.currentThread().getName());
                        try {
                            // Симуляция выполнения задачи (2 сек).
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        System.out.println("[MainDemo] Task " + taskId + " completed on " +
                                Thread.currentThread().getName());
                    }

                    @Override
                    public String toString() {
                        return "Task-" + taskId;
                    }
                });
            } catch (RejectedExecutionException e) {
                System.out.println("[MainDemo] Task " + taskId + " was rejected.");
            }
        }

        // Даем время на выполнение задач перед завершением пула.
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Инициируем корректное завершение пула.
        executor.shutdown();
        System.out.println("Executor shutdown initiated.");

        // Ожидаем завершения всех задач
        if (executor instanceof CustomThreadPool pool) {
            try {
                if (pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("[MainDemo] All tasks completed.");
                } else {
                    System.out.println("[MainDemo] Timeout: not all tasks completed.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[MainDemo] Await termination interrupted.");
            }
        }
    }
}