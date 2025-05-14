package com.amazonaws.services.msf.processor;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class EventDetector extends RichFlatMapFunction<String, String> {

    private transient ObjectMapper mapper;

    // 상태 값
    private transient ValueState<Double> lastVelocity;
    private transient ValueState<Long> zeroSince;

    private transient ValueState<Long> lastControlChangeTime;
    private transient ValueState<Double> lastThrottle;
    private transient ValueState<Double> lastBrake;
    private transient ValueState<Double> lastSteer;

    // 샘플 데이터
    private final Long userId = 1L;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        mapper = new ObjectMapper();
        lastVelocity = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVelocity", Double.class));
        zeroSince = getRuntimeContext().getState(new ValueStateDescriptor<>("zeroSince", Long.class));

        lastControlChangeTime = getRuntimeContext().getState(new ValueStateDescriptor<>("lastControlChangeTime", Long.class));
        lastThrottle = getRuntimeContext().getState(new ValueStateDescriptor<>("lastThrottle", Double.class));
        lastBrake = getRuntimeContext().getState(new ValueStateDescriptor<>("lastBrake", Double.class));
        lastSteer = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSteer", Double.class));
    }

    @Override
    public void flatMap(String json, Collector<String> out) throws Exception {
        Telemetry t = mapper.readValue(json, Telemetry.class);

        long now = System.currentTimeMillis();
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Asia/Seoul"));

        detectIdle(t, now, time, out);
        detectAccelOrDecel(t, now, time, out);
        detectOverspeed(t, time, out);
        detectInvasion(t, time, out);
        // 추후 detectNoControlInput(t, now, time, out) 등 추가 가능
    }

    // 공회전 감지
    private void detectIdle(Telemetry t, long now, LocalDateTime time, Collector<String> out) throws Exception {
        if (t.velocity != 0) {zeroSince.clear(); return;}
        Long since = zeroSince.value();
        if (since == null) {zeroSince.update(now); return;}
        if(now - since < 10_000) {return;}

        out.collect(getEventDTO(EventType.IDLE_ENGINE, t, time).toString());
        zeroSince.update(now);
    }

    // 급가/감속 감지
    private void detectAccelOrDecel(Telemetry t, long now, LocalDateTime time, Collector<String> out) throws Exception {
        Double prevVel = lastVelocity.value();
        lastVelocity.update(t.velocity);  // 상태 먼저 업데이트

        if (prevVel == null) return;

        double diff = t.velocity - prevVel;
        EventType type = diff >= 10 ? EventType.RAPID_ACCELERATION
                : diff <= -10 ? EventType.RAPID_DECELERATION
                : null;

        if (type == null) return;
        out.collect(getEventDTO(type, t, time).toString());

    }

    // 과속 감지
    private void detectOverspeed(Telemetry t, LocalDateTime time, Collector<String> out) {
        if (t.velocity < 50) return;
        out.collect(getEventDTO(EventType.OVERSPEED, t, time).toString());
    }

    // 차선 침범
    private void detectInvasion(Telemetry t, LocalDateTime time, Collector<String> out) {
        if (t.invasion == null || t.invasion.isEmpty()) return;
        out.collect(getEventDTO(EventType.INVASION, t, time).toString());
    }

    private Event getEventDTO(EventType type, Telemetry t, LocalDateTime time){
        return Event.builder()
                .userId(userId)
                .type(type.toString())
                .time(time)
                .gnssX(t.gnssX)
                .gnssY(t.gnssY)
                .driveId(t.driveId)
                .build();
    }
}
