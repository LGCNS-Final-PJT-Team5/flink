package com.amazonaws.services.msf.operator.overspeed;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import com.amazonaws.services.msf.util.EventFactory;
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
    private transient ValueState<Telemetry> lastTelemetry;

    @Override
    public void open(Configuration parameters) {
        overStart = getRuntimeContext().getState(new ValueStateDescriptor<>("overStart", Long.class));
        lastTelemetry = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTel", Telemetry.class));

    }

    // 과속 ≥ 100 km/h, 5 초 연속 유지 때마다 추가 알림
    @Override
    public void processElement(Telemetry t, Context ctx, Collector<Event> out) throws Exception {
        lastTelemetry.update(t);
        if (t.velocity < LIMIT_KMH) {overStart.clear(); return;}

        if (overStart.value() != null) {return;}

        // 최초 과속 감지
        overStart.update(ctx.timestamp());
        out.collect(EventFactory.from(t, EventType.OVERSPEED));
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + HOLD_MS);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {
        if (overStart.value() == null) return;

        Telemetry t = lastTelemetry.value();
        out.collect(EventFactory.from(t, EventType.OVERSPEED));

        // 5초 이후에도 계속 과속이면 또 알림
        ctx.timerService().registerEventTimeTimer(ts + HOLD_MS);
    }
}
