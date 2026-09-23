package com.cs324.worker;

import java.io.Serializable;
import java.util.UUID;

/** Message flooded through the worker network to elect the best coordinator candidate. */
public final class ElectionMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID messageId;
    private final int candidateWorkerId;
    private final int candidateJac;

    public ElectionMessage(int candidateWorkerId, int candidateJac) {
        this(UUID.randomUUID(), candidateWorkerId, candidateJac);
    }

    public ElectionMessage(UUID messageId, int candidateWorkerId, int candidateJac) {
        if (messageId == null) {
            throw new IllegalArgumentException("messageId must not be null");
        }
        this.messageId = messageId;
        this.candidateWorkerId = candidateWorkerId;
        this.candidateJac = candidateJac;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public int getCandidateWorkerId() {
        return candidateWorkerId;
    }

    public int getCandidateJac() {
        return candidateJac;
    }

    @Override
    public String toString() {
        return "ElectionMessage{"
                + "messageId=" + messageId
                + ", candidateWorkerId=" + candidateWorkerId
                + ", candidateJac=" + candidateJac
                + '}';
    }
}
