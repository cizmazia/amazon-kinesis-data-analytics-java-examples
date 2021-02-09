package com.amazonaws.services.kinesisanalytics;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import software.amazon.kinesis.connectors.flink.FlinkKinesisConsumer;
import software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants;

class BasicStreamingJobTest {
    @ClassRule
    public static MiniClusterResource flinkCluster =
        new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    @Test
    void test() throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        var inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-west-2");
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        inputProperties.setProperty("aws.endpoint", "http://localhost:4566");
        inputProperties.setProperty("aws.credentials.provider", "BASIC");
        inputProperties.setProperty("aws.credentials.provider.basic.accesskeyid", "test");
        inputProperties.setProperty("aws.credentials.provider.basic.secretkey", "test");
        var input = env.addSource(new FlinkKinesisConsumer<>("ExampleInputStream", new SimpleStringSchema(), inputProperties));

        input.print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
