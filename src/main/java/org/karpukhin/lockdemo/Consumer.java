package org.karpukhin.lockdemo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class Consumer implements Runnable {

    private final MessageRepo messageRepo;
    private final ExecutorService updateExecutor;

    private Consumer(MessageRepo messageRepo, ExecutorService updateExecutor) {
        this.messageRepo = Objects.requireNonNull(messageRepo, "messageRepo");
        this.updateExecutor = Objects.requireNonNull(updateExecutor, "updateExecutor");
    }

    @Override
    public void run() {
        try {
            runInternal();
        } catch (RuntimeException e) {
            log.error("Could not run task", e);
            throw e;
        }
    }

    private void runInternal() {
        List<Message> messages = findPendingMessages();
        if (messages.isEmpty()) {
            return;
        }

        AtomicInteger actual = new AtomicInteger(0);
        List<Future<Optional<Message>>> futures = messages.stream()
                .map(message -> updateExecutor.submit(new Updater(message)))
                .toList();
        futures.forEach(future -> {
            try {
                Optional<Message> message = future.get();
                if (message.isPresent()) {
                    log("Message was updated, id = %s", message.get().getId().toString());
                    actual.incrementAndGet();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Could not get result", e);
            }
        });
        log("%d/%d messages were updated", actual.get(), messages.size());
    }

    private List<Message> findPendingMessages() {
        return messageRepo.doInTransaction(connection -> {
            List<Message> messages = messageRepo.findPendingMessagesWithLock(connection);
            for (Message message : messages) {
                message.setStatus(Message.Status.SENDING);
            }
            messageRepo.updateMessages(connection, messages);
            return messages;
        });
    }

    public static void main(String[] args) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:55432/testdb");
        config.setUsername("testuser");
        config.setPassword("P@ssw0rd1+");

        DataSource dataSource = new HikariDataSource(config);
        ExecutorService executorService = Executors.newFixedThreadPool(10, new MyThreadFactory("consumer-thread-"));
        Consumer consumer = new Consumer(new MessageRepo(dataSource), executorService);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10/*, new MyThreadFactory("scheduler-thread-")*/);
        scheduler.scheduleAtFixedRate(consumer, 2000, 3000, TimeUnit.MILLISECONDS);
    }

    private static void log(String format, Object... args) {
        System.out.printf(format, args);
        System.out.println();
    }

    private static final class MyThreadFactory implements ThreadFactory {

        private final String namePrefix;

        private MyThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName(namePrefix + thread.getId());
            return thread;
        }
    }

    private final class Updater implements Callable<Optional<Message>> {

        private final Message message;

        private Updater(Message message) {
            this.message = message;
        }

        @Override
        public Optional<Message> call() {
            message.setStatus(Message.Status.SENT);
            messageRepo.updateMessage(message);
            return Optional.of(message);
        }
    }
}
