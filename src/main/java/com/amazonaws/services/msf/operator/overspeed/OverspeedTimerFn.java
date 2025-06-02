package com.amazonaws.services.msf.operator.overspeed;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class OverspeedTimerFn extends KeyedProcessFunction<String, Telemetry, Event> {

    private static final double LIMIT_KMH = 100.0;
    private static final long   HOLD_MS   = 5_000;

    private transient ValueState<Long> overStart;

    @Override
    public void open(Configuration parameters) {
        overStart = getRuntimeContext().getState(new ValueStateDescriptor<>("overStart", Long.class));
    }

    // 과속 ≥ 100 km/h, 5 초 연속 유지 때마다 추가 알림
    @Override
    public void processElement(Telemetry t, Context ctx, Collector<Event> out) throws Exception {
        if (t.velocity < LIMIT_KMH) {overStart.clear(); return;}

        if (overStart.value() != null) {return;}

        // 최초 과속 감지
        overStart.update(ctx.timestamp());
        out.collect(build(t));
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + HOLD_MS);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {
        if (overStart.value() == null) return;
        out.collect(Event.builder()
                .userId(ctx.getCurrentKey())
                .type(EventType.OVERSPEED.toString())
                .time(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Seoul")))                                      // 필요시 타임 변환
                .build());

        // 5초 이후에도 계속 과속이면 또 알림
        ctx.timerService().registerEventTimeTimer(ts + HOLD_MS);
    }

    private static Event build(Telemetry t) {
        return Event.builder()
                .userId(t.userId)
                .type(EventType.OVERSPEED.toString())
                .time(t.time)
                .build();
    }
}
