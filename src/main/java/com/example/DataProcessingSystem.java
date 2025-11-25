package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class DataProcessingSystem {

    // ----------------------------
    // Task model
    // ----------------------------
    static class Task {
        final int id;
        final int input;

        Task(int id, int input) {
            this.id = id;
            this.input = input;
        }
    }

    // ----------------------------
    // Shared queue with safe access
    // ----------------------------
    static class SharedTaskQueue {
        private final BlockingQueue<Task> queue;

        SharedTaskQueue(int capacity) {
            this.queue = new ArrayBlockingQueue<>(capacity);
        }

        void addTask(Task task) throws InterruptedException {
            queue.put(task); // blocks if full
        }

        Task getTask(long timeoutMs) throws InterruptedException {
            return queue.poll(timeoutMs, TimeUnit.MILLISECONDS); // null if timeout
        }
    }

    // ----------------------------
    // Shared results store + file output
    // ----------------------------
    static class ResultsStore {
        private final List<String> results = new ArrayList<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final BufferedWriter writer;

        ResultsStore(String outputPath) throws IOException {
            writer = new BufferedWriter(new FileWriter(outputPath, true));
        }

        void addResult(String result) {
            lock.lock();
            try {
                results.add(result);
                writer.write(result);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                log("ERROR writing to file: " + e.getMessage());
            } finally {
                lock.unlock();
            }
        }

        List<String> snapshot() {
            lock.lock();
            try {
                return new ArrayList<>(results);
            } finally {
                lock.unlock();
            }
        }

        void close() {
            lock.lock();
            try {
                writer.close();
            } catch (IOException e) {
                log("ERROR closing file: " + e.getMessage());
            } finally {
                lock.unlock();
            }
        }
    }

    // ----------------------------
    // Worker
    // ----------------------------
    static class Worker implements Runnable {
        private final int workerId;
        private final SharedTaskQueue taskQueue;
        private final ResultsStore resultsStore;

        Worker(int workerId, SharedTaskQueue taskQueue, ResultsStore resultsStore) {
            this.workerId = workerId;
            this.taskQueue = taskQueue;
            this.resultsStore = resultsStore;
        }

        @Override
        public void run() {
            log("Worker-" + workerId + " started.");
            try {
                while (true) {
                    Task task = taskQueue.getTask(300); // wait a bit for tasks
                    if (task == null) {
                        // no tasks recently → assume done
                        break;
                    }

                    // simulate processing delay
                    int processed = processTask(task);

                    String line = String.format(
                            "Worker-%d processed Task-%d (input=%d) -> output=%d",
                            workerId, task.id, task.input, processed
                    );
                    resultsStore.addResult(line);
                    log(line);
                }
            } catch (InterruptedException e) {
                log("Worker-" + workerId + " interrupted.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log("Worker-" + workerId + " ERROR: " + e.getMessage());
            } finally {
                log("Worker-" + workerId + " completed.");
            }
        }

        private int processTask(Task task) throws InterruptedException {
            Thread.sleep(150 + (task.input % 5) * 50L);
            // example computation: square + id
            return task.input * task.input + task.id;
        }
    }

    // ----------------------------
    // Main
    // ----------------------------
    public static void main(String[] args) {
        final int NUM_TASKS = 20;
        final int NUM_WORKERS = 4;
        final String OUTPUT_FILE = "java_results.txt";

        SharedTaskQueue taskQueue = new SharedTaskQueue(NUM_TASKS);
        ResultsStore resultsStore = null;

        try {
            resultsStore = new ResultsStore(OUTPUT_FILE);

            // Add tasks
            for (int i = 1; i <= NUM_TASKS; i++) {
                taskQueue.addTask(new Task(i, i * 3));
            }
            log("Enqueued " + NUM_TASKS + " tasks.");

            // Executor for managing workers
            ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
            for (int w = 1; w <= NUM_WORKERS; w++) {
                executor.submit(new Worker(w, taskQueue, resultsStore));
            }

            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log("Timeout waiting for workers. Forcing shutdown.");
                executor.shutdownNow();
            }

            log("All workers finished. Total results = " + resultsStore.snapshot().size());

        } catch (IOException e) {
            log("FATAL file I/O error: " + e.getMessage());
        } catch (InterruptedException e) {
            log("Main interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            if (resultsStore != null) resultsStore.close();
        }
    }

    static void log(String msg) {
        System.out.println("[" + LocalTime.now() + "] " + msg);
    }
}
