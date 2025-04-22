package com.codebodhi.sqslistener;

import java.time.Duration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsListenerConfig {
  Duration pollingFrequency;
  Duration visibilityTimeout;
  int parallelism;
  SqsAsyncClient sqsAsyncClient;

  private SqsListenerConfig() {}

  public static SqsListenerConfig builder() {
    return new SqsListenerConfig();
  }

  public SqsListenerConfig(
      Duration pollingFrequency,
      Duration visibilityTimeout,
      int parallelism,
      SqsAsyncClient sqsAsyncClient) {
    this.pollingFrequency = pollingFrequency;
    this.visibilityTimeout = visibilityTimeout;
    this.parallelism = parallelism;
    this.sqsAsyncClient = sqsAsyncClient;
  }

  public SqsListenerConfig pollingFrequency(Duration pollingFrequency) {
    this.pollingFrequency = pollingFrequency;
    return this;
  }

  public SqsListenerConfig visibilityTimeout(Duration visibilityTimeout) {
    this.visibilityTimeout = visibilityTimeout;
    return this;
  }

  public SqsListenerConfig parallelism(int parallelism) {
    if (parallelism < 0) {
      throw new SqsListenerException("Invalid value for parallelism! Valid values are from 1-10");
    } else if (parallelism > 10) {
      throw new UnsupportedOperationException("Currently parallelism up to 10 is only supported!");
    }
    this.parallelism = parallelism;
    return this;
  }

  public SqsListenerConfig sqsClient(SqsAsyncClient sqsAsyncClient) {
    this.sqsAsyncClient = sqsAsyncClient;
    return this;
  }

  public SqsListenerConfig build() {
    return new SqsListenerConfig(pollingFrequency, visibilityTimeout, parallelism, sqsAsyncClient);
  }
}
