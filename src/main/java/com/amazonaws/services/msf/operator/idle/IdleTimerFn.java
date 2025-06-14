package com.amazonaws.services.msf.operator.idle;

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

public class IdleTimerFn extends KeyedProcessFunction<String, Telemetry, Event> {
    private static final long IDLE_MS = 120_000;     // 2분
    private static final long INTERVAL_MS = 30_000;  // 이후 30초 간격 반복

    private ValueState<Long> idleStart;
    private transient ValueState<Telemetry> lastTelemetry;

    @Override
    public void open(Configuration parameters) throws Exception {
        idleStart = getRuntimeContext().getState(
                new ValueStateDescriptor<>("idleStart", Long.class));
        lastTelemetry  = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("lastTel",  Telemetry.class));

    }

    @Override
    public void processElement(Telemetry t, Context ctx, Collector<Event> out) throws Exception {
        lastTelemetry.update(t);
        if (t.velocity != 0) {idleStart.clear();return;}

        if (idleStart.value() != null) {return;}

        idleStart.update(ctx.timestamp());
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + IDLE_MS);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {
        if (idleStart.value() == null) return;

        Telemetry t = lastTelemetry.value();

        out.collect(EventFactory.from(t, EventType.IDLE_ENGINE));

        // 반복 알림을 위해 다음 타이머 재등록
        ctx.timerService().registerEventTimeTimer(ts + INTERVAL_MS);
    }
}
