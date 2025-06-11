package com.amazonaws.services.msf.sink;

import com.amazonaws.services.msf.dto.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class RdsSink {
    public static SinkFunction<Event> create(Properties props) {
        return JdbcSink.sink(
                "INSERT INTO event (user_id, drive_id, type, event_time, gnss_x, gnss_y) VALUES (?, ?, ?, ?, ?, ?)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement ps, Event e) throws SQLException {
                        ps.setString(1, e.getUserId());
                        ps.setString(2, e.getDriveId());
                        ps.setString(3, e.getEventType());
                        ps.setTimestamp(4, java.sql.Timestamp.valueOf(e.getTime()));
                        ps.setDouble(5, e.getGnssX());
                        ps.setDouble(6, e.getGnssY());

                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(props.getProperty("jdbc.url"))
                        .withDriverName(props.getProperty("jdbc.driver", "com.mysql.cj.jdbc.Driver"))
                        .withUsername(props.getProperty("jdbc.username"))
                        .withPassword(props.getProperty("jdbc.password"))
                        .build()
        );
    }
}
