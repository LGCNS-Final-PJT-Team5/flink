package com.amazonaws.services.msf;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.model.Telemetry;
import com.amazonaws.services.msf.operator.accel.AccelDecelFn;
import com.amazonaws.services.msf.operator.collision.CollisionFn;
import com.amazonaws.services.msf.operator.idle.IdleTimerFn;
import com.amazonaws.services.msf.operator.invasion.InvasionFn;
import com.amazonaws.services.msf.operator.nooperation.NoOpTimerFn;
import com.amazonaws.services.msf.operator.overspeed.OverspeedTimerFn;
import com.amazonaws.services.msf.operator.reactiondelay.ReactionDelayFn;
import com.amazonaws.services.msf.operator.safedistance.SafeDistanceFn;
import com.amazonaws.services.msf.operator.sharpturn.SharpTurnFn;
import com.amazonaws.services.msf.sink.RdsSink;
import com.amazonaws.services.msf.sink.SqsSink;
import com.amazonaws.services.msf.util.JsonMapper;
import com.amazonaws.services.msf.util.WatermarkFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class BasicStreamingJob {
    private static final Logger LOGGER = LogManager.getLogger(BasicStreamingJob.class);
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";
    private static final int TASK = 1;

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        final Map<String,Properties> appProps =
                BasicStreamingJob.loadApplicationProperties(env);

        // 1. Kinesis Source
        KeyedStream<Telemetry, String> keyed = env
                .setParallelism(TASK)// 각 연산자마다 N개의 task(연산자의 병렬 실행 인스턴스) 생성
                .addSource(BasicStreamingJob.createSource(appProps.get("InputStream0")))
                .name("Kinesis Source")
                .map(json -> {
                    // LOGGER.info("📥 Raw input from Kinesis: {}", json);
                    return JsonMapper.MAPPER.readValue(json, Telemetry.class);
                })
                .assignTimestampsAndWatermarks(WatermarkFactory.telemetry())
                .keyBy(Telemetry::getDriveId);


        // 2. 연산자 연결
        DataStream<Event> events = keyed
                .process(new IdleTimerFn()).name("Idle")
                .union(
                        keyed.flatMap(new AccelDecelFn()).name("AccelDecel"),
                        keyed.process(new NoOpTimerFn()).name("NoOp"),
                        keyed.process(new OverspeedTimerFn()).name("Overspeed"),
                        keyed.flatMap(new SharpTurnFn()).name("SharpTurn"),
                        keyed.flatMap(new InvasionFn()).name("Invasion"),
                        keyed.flatMap(new SafeDistanceFn()).name("SafeDistance"),
                        keyed.flatMap(new CollisionFn()).name("Collision"),
                        keyed.process(new ReactionDelayFn()).name("ReactionDelay")
                ).map(event -> {
                     // LOGGER.info("📤 Event generated: {}", event);
                    return event;
                });

        // 3. SQS Sinks
        String queueUrl = appProps.get("Sqs0").getProperty("queue.url");
        events.map(e -> {
                    String json = JsonMapper.MAPPER.writeValueAsString(e);
                    LOGGER.info("🚚 Sending to SQS: {}", json);
                    return json;
                })
                .addSink(new SqsSink(queueUrl))
                .name("SQS Sink");

        events.addSink(RdsSink.create(appProps.get("Rds0")))
                .name("RDS Sink");

        // 4. RDS Sinks
        env.execute("Vehicle-Event Pipeline");
    }

    private static DataStream<String> logInputData(DataStream<String> input) {
        return input.map(value -> {
            System.out.println(">>> EVENT: " + value.toString());
            return value;
        });
    }

}
