package com.codebodhi.sqslistener;

public class SqsListenerException extends RuntimeException {
  public SqsListenerException(String message) {
    super(message);
  }

  public SqsListenerException(String message, Throwable e) {
    super(message, e);
  }
}
