package org.karpukhin.lockdemo;

import java.util.UUID;
import lombok.Data;

@Data
class Message {
    private UUID id;
    private String text;
    private Status status;
    private int version;

    void increaseVersion() {
        ++version;
    }

    enum Status {
        PENDING,
        SENDING,
        SENT
    }
}
