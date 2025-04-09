package com.codebodhi.sqslistener;

import java.time.Instant;

class SqsMessage {
  String messageId;
  String receiptHandle;
  String body;
  Instant firstReceivedTimestamp;
  Integer receivedCount;

  SqsMessage(
      String messageId,
      String receiptHandle,
      String body,
      Instant firstReceivedTimestamp,
      Integer receivedCount) {
    this.messageId = messageId;
    this.receiptHandle = receiptHandle;
    this.body = body;
    this.firstReceivedTimestamp = firstReceivedTimestamp;
    this.receivedCount = receivedCount;
  }
}
