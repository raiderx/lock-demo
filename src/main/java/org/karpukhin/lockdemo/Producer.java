package org.karpukhin.lockdemo;

import com.github.f4b6a3.ulid.UlidCreator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class Producer {

    private final MessageRepo messageRepo;

    private Producer(MessageRepo messageRepo) {
        this.messageRepo = Objects.requireNonNull(messageRepo, "messageRepo");
    }

    private void run() {
        Scanner console = new Scanner(System.in);
        while (true) {
            System.out.print("Command (h for help): ");
            String command = console.nextLine();
            switch (command) {
                case "1":
                    sendMessages(1);
                    break;
                case "2":
                    sendMessages(2);
                    break;
                case "3":
                    sendMessages(3);
                    break;
                case "10":
                    sendMessages(10);
                    break;
                case "h":
                    printHelp();
                    break;
                case "q":
                    return;
                default:
                    System.out.println("Unknown command: " + command);
            }
        }
    }

    private void printHelp() {
        System.out.println("Usage:\n" +
                "h  help\n" +
                "1  send 1 message\n" +
                "2  send 2 messages\n" +
                "3  send 3 messages\n" +
                "10 send 10 messages\n" +
                "q  quit");
    }

    private void sendMessages(int count) {
        List<Message> messages = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            Message message = new Message();
            message.setId(UlidCreator.getUlid().toUuid());
            message.setText("Message " + i);
            message.setStatus(Message.Status.PENDING);
            messages.add(message);
        }
        saveMessages(messages);
        System.out.println(count + " messages were generated");
    }

    private void saveMessages(List<Message> messages) {
        messageRepo.doInTransaction(connection -> {
            messageRepo.saveMessages(connection, messages);
            return Optional.empty();
        });
    }

    public static void main(String[] args) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:55432/testdb");
        config.setUsername("testuser");
        config.setPassword("P@ssw0rd1+");

        DataSource dataSource = new HikariDataSource(config);
        MessageRepo messageRepo = new MessageRepo(dataSource);
        new Producer(messageRepo).run();
    }
}
