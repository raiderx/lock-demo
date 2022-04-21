package org.karpukhin.lockdemo;

import com.google.common.base.Suppliers;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class MessageRepo {

    private static final Supplier<String> PENDING_MESSAGES_WITH_LOCK = Suppliers.memoize(() -> sql("/pending_messages_with_lock.sql"));
    private static final Supplier<String> INSERT_MESSAGE = Suppliers.memoize(() -> sql("/insert_message.sql"));
    private static final Supplier<String> UPDATE_MESSAGE = Suppliers.memoize(() -> sql("/update_message.sql"));

    private final DataSource dataSource;

    MessageRepo(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    void saveMessages(Connection connection, List<Message> messages) {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_MESSAGE.get())) {
            for (Message message : messages) {
                int i = 0;
                statement.setObject(++i, message.getId());
                statement.setString(++i, message.getText());
                statement.setString(++i, message.getStatus().toString());
                statement.setInt(++i, 0);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Could not save messages", e);
        }
    }

    List<Message> findPendingMessagesWithLock(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(PENDING_MESSAGES_WITH_LOCK.get())) {
            statement.setString(1, Message.Status.PENDING.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                List<Message> result = new ArrayList<>();
                while (resultSet.next()) {
                    result.add(map(resultSet));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not fetch messages", e);
        }
    }

    void updateMessage(Message message) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPDATE_MESSAGE.get())) {
            int i = 0;
            statement.setString(++i, message.getText());
            statement.setString(++i, message.getStatus().toString());
            statement.setInt(++i, message.getVersion() + 1);
            statement.setObject(++i, message.getId());
            statement.setInt(++i, message.getVersion());
            int rows = statement.executeUpdate();
            if (rows != 1) {
                throw new IllegalStateException("Message was not updated, id: " + message.getId());
            }
            message.increaseVersion();
        } catch (SQLException e) {
            log.error("Could not update message", e);
        }
    }

    void updateMessages(Connection connection, List<Message> messages) {
        try (PreparedStatement statement = connection.prepareStatement(UPDATE_MESSAGE.get())) {
            for (Message message : messages) {
                int i = 0;
                statement.setString(++i, message.getText());
                statement.setString(++i, message.getStatus().toString());
                statement.setInt(++i, message.getVersion() + 1);
                statement.setObject(++i, message.getId());
                statement.setInt(++i, message.getVersion());
                statement.addBatch();
            }
            int[] rows = statement.executeBatch();
            Collection<UUID> failed = new HashSet<>();
            for (int i = 0; i < rows.length; ++i) {
                if (rows[i] != 1) {
                    failed.add(messages.get(i).getId());
                }
            }
            if (!failed.isEmpty()) {
                throw new IllegalStateException("Could not update messages, ids: " + failed);
            }
            messages.forEach(Message::increaseVersion);

        } catch (SQLException e) {
            log.error("Could not update messages", e);
        }
    }

    <T> T doInTransaction(TransactionTemplate<T> template) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = template.doInTransaction(connection);
                connection.commit();
                return result;
            } catch (Exception e) {
                connection.rollback();
                throw new RuntimeException("Could not run", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error", e);
        }
    }

    private static Message map(ResultSet resultSet) throws SQLException {
        Message message = new Message();
        message.setId(resultSet.getObject("id", UUID.class));
        message.setText(resultSet.getString("message"));
        message.setStatus(Message.Status.valueOf(resultSet.getString("status")));
        message.setVersion(resultSet.getInt("version"));
        return message;
    }

    private static String sql(String path) {
        try (Reader reader = new InputStreamReader(Objects.requireNonNull(MessageRepo.class.getResourceAsStream(path)), StandardCharsets.UTF_8)) {
            return CharStreams.toString(reader);
        } catch (IOException e) {
            throw new RuntimeException("Could not read " + path, e);
        }
    }
}
