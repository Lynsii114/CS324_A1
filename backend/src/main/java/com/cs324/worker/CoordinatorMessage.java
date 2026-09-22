package com.cs324.worker;

import java.io.Serializable;
import java.util.UUID;

/** Message sent through the worker network after a coordinator is elected. */
public final class CoordinatorMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID messageId;
    private final int coordinatorId;

    public CoordinatorMessage(int coordinatorId) {
        this(UUID.randomUUID(), coordinatorId);
    }

    public CoordinatorMessage(UUID messageId, int coordinatorId) {
        if (messageId == null) {
            throw new IllegalArgumentException("messageId must not be null");
        }
        this.messageId = messageId;
        this.coordinatorId = coordinatorId;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public int getCoordinatorId() {
        return coordinatorId;
    }

    @Override
    public String toString() {
        return "CoordinatorMessage{"
                + "messageId=" + messageId
                + ", coordinatorId=" + coordinatorId
                + '}';
    }
}
