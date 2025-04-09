package com.codebodhi.sqslistener;

import java.time.Instant;

record SqsMessage(
    String messageId,
    String receiptHandle,
    String body,
    Instant firstReceivedTimestamp,
    Integer receivedCount) {}
