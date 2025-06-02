package com.amazonaws.services.msf.operator.nooperation;

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


public class NoOpTimerFn extends KeyedProcessFunction<String, Telemetry, Event> {

    private static final long NOOP_MS = 3_000;

    private transient ValueState<Long> lastChanged;
    private transient ValueState<Double> lastThrottle;
    private transient ValueState<Double> lastBrake;
    private transient ValueState<Double> lastSteer;

    @Override
    public void open(Configuration parameters) {
        lastChanged   = getRuntimeContext().getState(new ValueStateDescriptor<>("lastChanged", Long.class));
        lastThrottle  = getRuntimeContext().getState(new ValueStateDescriptor<>("lastThrottle", Double.class));
        lastBrake     = getRuntimeContext().getState(new ValueStateDescriptor<>("lastBrake", Double.class));
        lastSteer     = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSteer", Double.class));
    }


    // 조작 없음 3 초 지속 — 3 초마다 재알림
    @Override
    public void processElement(Telemetry t, Context ctx, Collector<Event> out) throws Exception {
        boolean changed = false;

        if (!equals(lastThrottle.value(), t.Throttle)) { lastThrottle.update(t.Throttle); changed = true; }
        if (!equals(lastBrake.value(),    t.Brake))    { lastBrake.update(t.Brake);       changed = true; }
        if (!equals(lastSteer.value(),    t.Steer))    { lastSteer.update(t.Steer);       changed = true; }

        if (changed) {
            lastChanged.update(ctx.timestamp());
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + NOOP_MS);
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {
        Long last = lastChanged.value();
        if (last == null || ts - last < NOOP_MS) return;

        out.collect(Event.builder()
                .userId(ctx.getCurrentKey())
                .type(EventType.NO_OPERATION.toString())
                .time(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Seoul")))
                .build());

        // 다음 3초 후 재알림
        ctx.timerService().registerEventTimeTimer(ts + NOOP_MS);
    }

    private static boolean equals(Double a, Double b) {
        return a == null ? b == null : a.equals(b);
    }
}
