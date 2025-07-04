package com.amazonaws.services.msf.operator.nooperation;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import com.amazonaws.services.msf.util.EventFactory;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class NoOpTimerFn extends KeyedProcessFunction<String, Telemetry, Event> {

    private static final long NOOP_MS = 3_000;

    private transient ValueState<Long>       lastChanged;
    private transient ValueState<Double>     lastThrottle;
    private transient ValueState<Double>     lastBrake;
    private transient ValueState<Double>     lastSteer;
    private transient ValueState<Telemetry>  lastTelemetry;

    @Override
    public void open(Configuration parameters) {
        lastChanged   = getRuntimeContext().getState(new ValueStateDescriptor<>("lastChanged",   Long.class));
        lastThrottle  = getRuntimeContext().getState(new ValueStateDescriptor<>("lastThrottle",  Double.class));
        lastBrake     = getRuntimeContext().getState(new ValueStateDescriptor<>("lastBrake",     Double.class));
        lastSteer     = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSteer",     Double.class));
        lastTelemetry = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTelemetry", Telemetry.class));
    }

    /* ───────────────────────────────────── processElement ──────────────────────────────────── */
    @Override
    public void processElement(Telemetry t, Context ctx, Collector<Event> out) throws Exception {
        lastTelemetry.update(t);

        /* 1) 주행 중이 아니면 감시 초기화 */
        if (t.velocity <= 0) {
            cancelPreviousTimer(ctx);
            clearAllStates();
            return;
        }

        /* 2) 스로틀·브레이크·스티어 값 변동 체크 */
        boolean changed = false;
        if (!equals(lastThrottle.value(), t.Throttle)) { lastThrottle.update(t.Throttle); changed = true; }
        if (!equals(lastBrake.value(),    t.Brake))    { lastBrake.update(t.Brake);       changed = true; }
        if (!equals(lastSteer.value(),    t.Steer))    { lastSteer.update(t.Steer);       changed = true; }

        if (!changed) return;   // 조작 변동 없음 → 그대로 감시

        /* 3) 조작 변동 시: 타이머 리셋 */
        long now = ctx.timestamp();
        cancelPreviousTimer(ctx);                 // 기존 타이머(있다면) 제거
        lastChanged.update(now);
        ctx.timerService().registerEventTimeTimer(now + NOOP_MS);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {
        Telemetry t = lastTelemetry.value();
        if (t == null || t.velocity <= 0) return;

        Long last = lastChanged.value();
        if (last == null || ts - last < NOOP_MS) return;   // 3 초 미도달

        /* 3 초 무조작 상태 → 이벤트 발생 */
        out.collect(EventFactory.from(t, EventType.NO_OPERATION));

        /* 타임스탬프 갱신 후 3 초 뒤 재타이머 → 정확히 3 초마다 알림 */
        lastChanged.update(ts);
        ctx.timerService().registerEventTimeTimer(ts + NOOP_MS);
    }

    private static boolean equals(Double a, Double b) {
        return a == null ? b == null : a.equals(b);
    }

    private void cancelPreviousTimer(Context ctx) throws Exception {
        Long last = lastChanged.value();
        if (last != null) {
            ctx.timerService().deleteEventTimeTimer(last + NOOP_MS);
        }
    }

    private void clearAllStates() throws Exception {
        lastChanged.clear();
        lastThrottle.clear();
        lastBrake.clear();
        lastSteer.clear();
    }
}
