package com.codebodhi.sqslistener;

import java.time.Duration;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsListenerConfig {
  Duration pollingFrequency;
  Duration visibilityTimeout;
  int parallelism;
  SqsClient sqsClient;

  private SqsListenerConfig() {}

  public static SqsListenerConfig builder() {
    return new SqsListenerConfig();
  }

  public SqsListenerConfig(
      Duration pollingFrequency, Duration visibilityTimeout, int parallelism, SqsClient sqsClient) {
    this.pollingFrequency = pollingFrequency;
    this.visibilityTimeout = visibilityTimeout;
    this.parallelism = parallelism;
    this.sqsClient = sqsClient;
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

  public SqsListenerConfig sqsClient(SqsClient sqsClient) {
    this.sqsClient = sqsClient;
    return this;
  }

  public SqsListenerConfig build() {
    return new SqsListenerConfig(pollingFrequency, visibilityTimeout, parallelism, sqsClient);
  }
}
