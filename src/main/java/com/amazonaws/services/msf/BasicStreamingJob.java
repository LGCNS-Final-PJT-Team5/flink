package com.amazonaws.services.msf;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.processor.EventDetector;
import com.amazonaws.services.msf.sink.RdsSink;
import com.amazonaws.services.msf.sink.SqsSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class BasicStreamingJob {
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final Logger LOGGER = LogManager.getLogger(BasicStreamingJob.class);
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            LOGGER.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            return KinesisAnalyticsRuntime.getApplicationProperties(
                    BasicStreamingJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOGGER.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }

    private static FlinkKinesisConsumer<String> createSource(Properties inputProperties) {
        String inputStreamName = inputProperties.getProperty("stream.name");
        return new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties);
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Map<String, Properties> applicationParameters = loadApplicationProperties(env);

        SourceFunction<String> source = createSource(applicationParameters.get("InputStream0"));
        DataStream<String> raw = env.addSource(source, "Kinesis Source");

        // 1) 이벤트 감지
        DataStream<String> detected = raw
                .keyBy(value -> 0)
                .flatMap(new EventDetector())
                .name("Event Detector");

        // 2) 로그 찍고
        DataStream<String> printed = logInputData(detected);

        // 3) SQS 전송

        DataStream<Event> events = detected
                .map(json -> mapper.readValue(json, Event.class), TypeInformation.of(Event.class))
                .name("Json → Event");

        events.addSink(RdsSink.create(applicationParameters.get("Rds0")))
                .name("RDS Sink");

        env.execute("Kinesis → Event Detection → SQS");
    }

    private static DataStream<String> logInputData(DataStream<String> input) {
        return input.map(value -> {
            System.out.println(">>> EVENT: " + value.toString());
            return value;
        });
    }

}
