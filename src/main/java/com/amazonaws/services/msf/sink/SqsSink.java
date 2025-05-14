package com.amazonaws.services.msf.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;


public class SqsSink extends RichSinkFunction<String> {

    private transient SqsClient sqsClient;
    private final String queueUrl;

    public SqsSink(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public void open(Configuration parameters) {
        this.sqsClient = SqsClient.builder().build();
    }

    @Override
    public void invoke(String value, Context context) {
        sqsClient.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(value)
                        .build()
        );
    }

    @Override
    public void close() {
        if (sqsClient != null) sqsClient.close();
    }
}