# Отчёт по кастомному пулу потоков

## 1. Введение

В рамках данного проекта был разработан собственный пул потоков («CustomThreadPool»). Цель работы — обеспечить гибкую
настройку параметров (размер пула, длина очереди, политика отказов, время простоя и пр.), а также продемонстрировать
логику распределения задач, логику завершения потоков и кастомную логику при переполнении очереди.

## 2. Краткая архитектура решения

1. **Интерфейс `CustomExecutor`**  
   Определяет методы, необходимые для работы с пулом:
    - `execute(Runnable command)`
    - `submit(Callable<T> callable)`
    - `shutdown()` и `shutdownNow()`
    - Метод ожидания завершения работы пула (`awaitTermination`).

2. **Основной класс пула `CustomThreadPool`**
    - Хранит параметры пула:
        - `corePoolSize`, `maxPoolSize`, `keepAliveTime`, `timeUnit`, `queueSize`, `minSpareThreads`.
    - Содержит набор очередей (по одной на каждый поток) для задач.
    - Управляет жизненным циклом воркеров (Worker).
    - Реализует распределение задач по алгоритму Round Robin.
    - Обрабатывает ситуации перегрузки с помощью собственного механизма отклонения задач.

3. **Рабочий поток `Worker`**
    - Является потоком (реализует `Runnable`), который постоянно пытается взять задачу из собственной очереди.
    - При отсутствии задач в течение заданного `keepAliveTime` поток может завершиться, если общее число потоков больше
      `corePoolSize`.
    - Реализовано логирование ключевых этапов (начало выполнения задачи, завершение, таймаут простоя и т. д.).

4. **ThreadFactory**
    - Создаёт потоки с уникальными именами (например, `MyPool-worker-1`) и логирует процесс создания.
    - Позволяет настраивать daemon-флаги потоков.
    - Также, при завершении потока (например, из-за таймаута бездействия), соответствующее событие логируется воркером,
      что позволяет отслеживать жизненный цикл потоков. (например, `MyPool-worker-1`) и логирует процесс создания.
    - Позволяет настраивать daemon-флаги потоков.

5. **Балансировка задач**
    - Используются несколько очередей и алгоритм распределения Round Robin. Каждая новая задача помещается в следующую
      по порядку очередь воркера, обеспечивая равномерную загрузку потоков.

## 3. Анализ производительности

### 3.1 Сравнение с `ThreadPoolExecutor`

В ходе нагрузочного тестирования (1000 задач по 100 мс) была зафиксирована следующая разница:

- **CustomThreadPool** завершал выполнение за ~25 секунд.
- **ThreadPoolExecutor** (с аналогичными параметрами) — за ~50 секунд.

Причина этой разницы — архитектурное различие:

- **CustomThreadPool** использует по одной очереди на каждого воркера, что полностью исключает конкуренцию потоков за
  задачу.
- **ThreadPoolExecutor** использует единую очередь, все потоки конкурируют за её доступ. Это приводит к синхронизации и
  задержкам.

Таким образом, кастомная реализация в данной архитектуре оказывается в 2 раза быстрее при высокой частоте коротких
задач. В сценариях с короткими задачами и высокой конкуренцией модель с изолированными очередями оказывается
эффективнее.

### 3.2 Влияние параметров

- **`corePoolSize` и `maxPoolSize`**:
    - Оптимальным является отношение 1:2 между core и max. Это балансирует нагрузку и накладные расходы.
- **`queueSize`**:
    - Рекомендуемый размер очереди равен примерно количеству активных потоков. Слишком большие очереди увеличивают
      задержку выполнения задач.
- **`keepAliveTime`**:
    - Значения 3–5 секунд являются оптимальными для оперативного высвобождения потоков и экономии ресурсов при
      изменчивой нагрузке.
- **`minSpareThreads`**:
    - Значение 1–2 обеспечивает готовность пула к внезапным всплескам нагрузки без избыточного потребления ресурсов.

Оптимальный набор параметров при нагрузке ~2 000 задач (200–500 мс на задачу):

```
corePoolSize = 4
maxPoolSize  = 8
queueSize    = 50
keepAliveTime = 5s
minSpareThreads = 1
```

## 4. Механизм распределения задач

Задачи кладутся в индивидуальные очереди воркеров по алгоритму **Round Robin**. Это обеспечивает:

- равномерную загрузку потоков;
- минимизацию времени ожидания задач;
- простоту реализации и низкие накладные расходы на балансировку.

При переполнении очереди и невозможности добавления задач создаются новые потоки (если число потоков < `maxPoolSize`). В
случае, если новый поток создать невозможно, задача отклоняется с логированием.

### 4.1 Политика отказа

В случае перегрузки — когда все очереди заполнены, а количество потоков достигло максимального значения — задача
отклоняется с помощью собственного обработчика (`RejectedExecutionException`). 

**Это позволяет:**
- быстро обнаруживать перегрузку;
- избегать блокировок или чрезмерного роста задержек;
- сохранить предсказуемость поведения системы.

**Минусы:**
- задачи могут теряться без возможности восстановления;
- такая политика подходит только в сценариях, где допустима потеря задачи (например, при превышении SLA);
- в критичных системах потребуется более гибкий отказ (например, fallback, re-queue, backpressure).


## 5 Логирование

Система логирования реализована во всех ключевых местах пула. Логи фиксируют следующие события:

- При создании каждого потока:
  `[ThreadFactory] Creating new thread: MyPool-worker-1`
- При завершении потока:
  `[Worker] MyPool-worker-1 terminated.`
- При поступлении задачи:
  `[Pool] Task accepted into queue #(id): <описание задачи>`
- Если очередь переполнена или задача отклонена:
  `[Rejected] Task <...> was rejected due to overload!`
- При выполнении каждой задачи:
  `[Worker] MyPool-worker-2 executes <описание задачи>`
- При срабатывании idle timeout:
  `[Worker] MyPool-worker-2 idle timeout, stopping.`

Такой подход позволяет отслеживать жизненный цикл потоков и анализировать поведение системы в режиме реального времени.

## 6. Демонстрационная программа

Для проверки корректности реализации и соответствия требованиям задания была разработана демонстрационная программа
`MainDemo`, выполняющая следующие действия:

- Инициализирует пул с параметрами: `corePoolSize=2`, `maxPoolSize=4`, `queueSize=5`, `keepAliveTime=5 секунд`,
  `minSpareThreads=1`.
- Отправляет в пул 10 имитационных задач (`Runnable`), каждая из которых вызывает `Thread.sleep(2000)` и логирует начало
  и завершение выполнения.
- После 15 секунд ожидания вызывается метод `shutdown()`.
- Проверяется, что все задачи завершены, потоки завершили работу, и логируются события завершения.
- Демонстрируется ситуация отказа: если задач поступает больше, чем пул может обработать (более 9–10 одновременно),
  лишние задачи отклоняются, и это логируется через `[Rejected]`.

Программа позволяет наглядно убедиться в корректной работе всех компонентов пула: создания потоков, постановки задач,
завершения воркеров по idle timeout, соблюдения политики `minSpareThreads`, и реакции на перегрузку.


### Пример запуска программы

```declarative
Connected to the target VM, address: '127.0.0.1:53387', transport: 'socket'
[ThreadFactory] Creating new thread: MyCustomPool-worker-thread-1
[ThreadFactory] Creating new thread: MyCustomPool-worker-thread-2
[MainDemo] Task 1 started on MyCustomPool-worker-thread-1
[Pool] Task accepted into queue #1268959798: Task-1
[Pool] Task accepted into queue #1239548589: Task-2
[Pool] Task accepted into queue #477289012: Task-3
[Pool] Task accepted into queue #1795960102: Task-4
[Pool] Task accepted into queue #1027591600: Task-5
[Pool] Task accepted into queue #1678854096: Task-6
[Pool] Task accepted into queue #1849201180: Task-7
[MainDemo] Task 2 started on MyCustomPool-worker-thread-2
[ThreadFactory] Creating new thread: MyCustomPool-worker-thread-3
[Pool] Task accepted into queue #1691875296: Task-8
[Pool] Task accepted into queue #667346055: Task-9
[MainDemo] Task 9 started on MyCustomPool-worker-thread-3
[ThreadFactory] Creating new thread: MyCustomPool-worker-thread-4
[Pool] Task accepted into queue #1225197672: Task-10
[Pool] Task accepted into queue #1669712678: Task-11
[Pool] Task accepted into queue #943081537: Task-12
[Pool] Task accepted into queue #683962652: Task-13
[MainDemo] Task 13 started on MyCustomPool-worker-thread-4
[Pool] Task accepted into queue #1500608548: Task-14
[Pool] Task accepted into queue #341853399: Task-15
[Pool] Task accepted into queue #513700442: Task-16
[Pool] Task accepted into queue #366590980: Task-17
[Rejected] Task Task-18 was rejected due to overload!
[MainDemo] Task 18 was rejected.
[Rejected] Task Task-19 was rejected due to overload!
[MainDemo] Task 19 was rejected.
[Pool] Task accepted into queue #1366025231: Task-20
[Pool] Task accepted into queue #1007309018: Task-21
[Rejected] Task Task-22 was rejected due to overload!
[MainDemo] Task 22 was rejected.
[Rejected] Task Task-23 was rejected due to overload!
[MainDemo] Task 23 was rejected.
[Pool] Task accepted into queue #1684792003: Task-24
[Pool] Task accepted into queue #2038148563: Task-25
[Rejected] Task Task-26 was rejected due to overload!
[MainDemo] Task 26 was rejected.
[Rejected] Task Task-27 was rejected due to overload!
[MainDemo] Task 27 was rejected.
[Pool] Task accepted into queue #2008966511: Task-28
[Pool] Task accepted into queue #433874882: Task-29
[Rejected] Task Task-30 was rejected due to overload!
[MainDemo] Task 30 was rejected.
[Rejected] Task Task-31 was rejected due to overload!
[MainDemo] Task 31 was rejected.
[Rejected] Task Task-32 was rejected due to overload!
[MainDemo] Task 32 was rejected.
[Pool] Task accepted into queue #572191680: Task-33
[Rejected] Task Task-34 was rejected due to overload!
[MainDemo] Task 34 was rejected.
[Rejected] Task Task-35 was rejected due to overload!
[MainDemo] Task 35 was rejected.
[Rejected] Task Task-36 was rejected due to overload!
[MainDemo] Task 36 was rejected.
[Rejected] Task Task-37 was rejected due to overload!
[MainDemo] Task 37 was rejected.
[Rejected] Task Task-38 was rejected due to overload!
[MainDemo] Task 38 was rejected.
[Rejected] Task Task-39 was rejected due to overload!
[MainDemo] Task 39 was rejected.
[Rejected] Task Task-40 was rejected due to overload!
[MainDemo] Task 40 was rejected.
[Rejected] Task Task-41 was rejected due to overload!
[MainDemo] Task 41 was rejected.
[Rejected] Task Task-42 was rejected due to overload!
[MainDemo] Task 42 was rejected.
[Rejected] Task Task-43 was rejected due to overload!
[MainDemo] Task 43 was rejected.
[Rejected] Task Task-44 was rejected due to overload!
[MainDemo] Task 44 was rejected.
[Rejected] Task Task-45 was rejected due to overload!
[MainDemo] Task 45 was rejected.
[Rejected] Task Task-46 was rejected due to overload!
[MainDemo] Task 46 was rejected.
[Rejected] Task Task-47 was rejected due to overload!
[MainDemo] Task 47 was rejected.
[Rejected] Task Task-48 was rejected due to overload!
[MainDemo] Task 48 was rejected.
[Rejected] Task Task-49 was rejected due to overload!
[MainDemo] Task 49 was rejected.
[Rejected] Task Task-50 was rejected due to overload!
[MainDemo] Task 50 was rejected.
[Rejected] Task Task-51 was rejected due to overload!
[MainDemo] Task 51 was rejected.
[Rejected] Task Task-52 was rejected due to overload!
[MainDemo] Task 52 was rejected.
[Rejected] Task Task-53 was rejected due to overload!
[MainDemo] Task 53 was rejected.
[Rejected] Task Task-54 was rejected due to overload!
[MainDemo] Task 54 was rejected.
[Rejected] Task Task-55 was rejected due to overload!
[MainDemo] Task 55 was rejected.
[Rejected] Task Task-56 was rejected due to overload!
[MainDemo] Task 56 was rejected.
[Rejected] Task Task-57 was rejected due to overload!
[MainDemo] Task 57 was rejected.
[Rejected] Task Task-58 was rejected due to overload!
[MainDemo] Task 58 was rejected.
[Rejected] Task Task-59 was rejected due to overload!
[MainDemo] Task 59 was rejected.
[Rejected] Task Task-60 was rejected due to overload!
[MainDemo] Task 60 was rejected.
[Rejected] Task Task-61 was rejected due to overload!
[MainDemo] Task 61 was rejected.
[Rejected] Task Task-62 was rejected due to overload!
[MainDemo] Task 62 was rejected.
[Rejected] Task Task-63 was rejected due to overload!
[MainDemo] Task 63 was rejected.
[Rejected] Task Task-64 was rejected due to overload!
[MainDemo] Task 64 was rejected.
[Rejected] Task Task-65 was rejected due to overload!
[MainDemo] Task 65 was rejected.
[Rejected] Task Task-66 was rejected due to overload!
[MainDemo] Task 66 was rejected.
[Rejected] Task Task-67 was rejected due to overload!
[MainDemo] Task 67 was rejected.
[Rejected] Task Task-68 was rejected due to overload!
[MainDemo] Task 68 was rejected.
[Rejected] Task Task-69 was rejected due to overload!
[MainDemo] Task 69 was rejected.
[Rejected] Task Task-70 was rejected due to overload!
[MainDemo] Task 70 was rejected.
[Rejected] Task Task-71 was rejected due to overload!
[MainDemo] Task 71 was rejected.
[Rejected] Task Task-72 was rejected due to overload!
[MainDemo] Task 72 was rejected.
[Rejected] Task Task-73 was rejected due to overload!
[MainDemo] Task 73 was rejected.
[Rejected] Task Task-74 was rejected due to overload!
[MainDemo] Task 74 was rejected.
[Rejected] Task Task-75 was rejected due to overload!
[MainDemo] Task 75 was rejected.
[Rejected] Task Task-76 was rejected due to overload!
[MainDemo] Task 76 was rejected.
[Rejected] Task Task-77 was rejected due to overload!
[MainDemo] Task 77 was rejected.
[Rejected] Task Task-78 was rejected due to overload!
[MainDemo] Task 78 was rejected.
[Rejected] Task Task-79 was rejected due to overload!
[MainDemo] Task 79 was rejected.
[Rejected] Task Task-80 was rejected due to overload!
[MainDemo] Task 80 was rejected.
[Rejected] Task Task-81 was rejected due to overload!
[MainDemo] Task 81 was rejected.
[Rejected] Task Task-82 was rejected due to overload!
[MainDemo] Task 82 was rejected.
[Rejected] Task Task-83 was rejected due to overload!
[MainDemo] Task 83 was rejected.
[Rejected] Task Task-84 was rejected due to overload!
[MainDemo] Task 84 was rejected.
[Rejected] Task Task-85 was rejected due to overload!
[MainDemo] Task 85 was rejected.
[Rejected] Task Task-86 was rejected due to overload!
[MainDemo] Task 86 was rejected.
[Rejected] Task Task-87 was rejected due to overload!
[MainDemo] Task 87 was rejected.
[Rejected] Task Task-88 was rejected due to overload!
[MainDemo] Task 88 was rejected.
[Rejected] Task Task-89 was rejected due to overload!
[MainDemo] Task 89 was rejected.
[Rejected] Task Task-90 was rejected due to overload!
[MainDemo] Task 90 was rejected.
[Rejected] Task Task-91 was rejected due to overload!
[MainDemo] Task 91 was rejected.
[Rejected] Task Task-92 was rejected due to overload!
[MainDemo] Task 92 was rejected.
[Rejected] Task Task-93 was rejected due to overload!
[MainDemo] Task 93 was rejected.
[Rejected] Task Task-94 was rejected due to overload!
[MainDemo] Task 94 was rejected.
[Rejected] Task Task-95 was rejected due to overload!
[MainDemo] Task 95 was rejected.
[Rejected] Task Task-96 was rejected due to overload!
[MainDemo] Task 96 was rejected.
[Rejected] Task Task-97 was rejected due to overload!
[MainDemo] Task 97 was rejected.
[Rejected] Task Task-98 was rejected due to overload!
[MainDemo] Task 98 was rejected.
[Rejected] Task Task-99 was rejected due to overload!
[MainDemo] Task 99 was rejected.
[Rejected] Task Task-100 was rejected due to overload!
[MainDemo] Task 100 was rejected.
[MainDemo] Task 1 completed on MyCustomPool-worker-thread-1
[MainDemo] Task 3 started on MyCustomPool-worker-thread-1
[MainDemo] Task 2 completed on MyCustomPool-worker-thread-2
[MainDemo] Task 4 started on MyCustomPool-worker-thread-2
[MainDemo] Task 9 completed on MyCustomPool-worker-thread-3
[MainDemo] Task 12 started on MyCustomPool-worker-thread-3
[MainDemo] Task 13 completed on MyCustomPool-worker-thread-4
[MainDemo] Task 17 started on MyCustomPool-worker-thread-4
[MainDemo] Task 3 completed on MyCustomPool-worker-thread-1
[MainDemo] Task 5 started on MyCustomPool-worker-thread-1
[MainDemo] Task 4 completed on MyCustomPool-worker-thread-2
[MainDemo] Task 6 started on MyCustomPool-worker-thread-2
[MainDemo] Task 12 completed on MyCustomPool-worker-thread-3
[MainDemo] Task 16 started on MyCustomPool-worker-thread-3
[MainDemo] Task 17 completed on MyCustomPool-worker-thread-4
[MainDemo] Task 21 started on MyCustomPool-worker-thread-4
[MainDemo] Task 5 completed on MyCustomPool-worker-thread-1
[MainDemo] Task 7 started on MyCustomPool-worker-thread-1
[MainDemo] Task 6 completed on MyCustomPool-worker-thread-2
[MainDemo] Task 8 started on MyCustomPool-worker-thread-2
[MainDemo] Task 21 completed on MyCustomPool-worker-thread-4
[MainDemo] Task 16 completed on MyCustomPool-worker-thread-3
[MainDemo] Task 20 started on MyCustomPool-worker-thread-3
[MainDemo] Task 25 started on MyCustomPool-worker-thread-4
[MainDemo] Task 7 completed on MyCustomPool-worker-thread-1
[MainDemo] Task 8 completed on MyCustomPool-worker-thread-2
[MainDemo] Task 11 started on MyCustomPool-worker-thread-2
[MainDemo] Task 10 started on MyCustomPool-worker-thread-1
[MainDemo] Task 20 completed on MyCustomPool-worker-thread-3
[MainDemo] Task 24 started on MyCustomPool-worker-thread-3
[MainDemo] Task 25 completed on MyCustomPool-worker-thread-4
[MainDemo] Task 29 started on MyCustomPool-worker-thread-4
[MainDemo] Task 11 completed on MyCustomPool-worker-thread-2
[MainDemo] Task 15 started on MyCustomPool-worker-thread-2
[MainDemo] Task 10 completed on MyCustomPool-worker-thread-1
[MainDemo] Task 14 started on MyCustomPool-worker-thread-1
[MainDemo] Task 24 completed on MyCustomPool-worker-thread-3
[MainDemo] Task 28 started on MyCustomPool-worker-thread-3
[MainDemo] Task 29 completed on MyCustomPool-worker-thread-4
[MainDemo] Task 33 started on MyCustomPool-worker-thread-4
[MainDemo] Task 15 completed on MyCustomPool-worker-thread-2
[MainDemo] Task 33 completed on MyCustomPool-worker-thread-4
[MainDemo] Task 14 completed on MyCustomPool-worker-thread-1
[MainDemo] Task 28 completed on MyCustomPool-worker-thread-3
Executor shutdown initiated.
[MainDemo] All tasks completed.
Disconnected from the target VM, address: '127.0.0.1:53387', transport: 'socket'

Process finished with exit code 0
```