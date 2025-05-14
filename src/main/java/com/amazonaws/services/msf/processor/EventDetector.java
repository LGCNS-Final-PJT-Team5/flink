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
    private transient ValueState<Boolean> lastInvasionState;
    private transient ValueState<Boolean> wasOverspeed;
    private transient ValueState<Boolean> wasRapidAccel;
    private transient ValueState<Boolean> wasRapidDecel;
    private transient ValueState<Boolean> wasCollision;
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
        lastInvasionState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastInvasionState", Boolean.class));
        wasOverspeed = getRuntimeContext().getState(new ValueStateDescriptor<>("wasOverspeed", Boolean.class));
        wasRapidAccel = getRuntimeContext().getState(new ValueStateDescriptor<>("wasRapidAccel", Boolean.class));
        wasRapidDecel = getRuntimeContext().getState(new ValueStateDescriptor<>("wasRapidDecel", Boolean.class));
        wasCollision = getRuntimeContext().getState(new ValueStateDescriptor<>("wasCollision", Boolean.class));

    }

    @Override
    public void flatMap(String json, Collector<String> out) throws Exception {
        Telemetry t = mapper.readValue(json, Telemetry.class);

        long now = System.currentTimeMillis();
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Asia/Seoul"));

        detectIdle(t, now, time, out);
        // detectAccelOrDecel(t, time, out);
        detectOverspeed(t, time, out);
        detectInvasion(t, time, out);
        detectCollision(t, time, out);
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

    private void detectAccelOrDecel(Telemetry t, LocalDateTime time, Collector<String> out) throws Exception {
        Double prevVel = lastVelocity.value();
        lastVelocity.update(t.velocity);  // 상태 먼저 업데이트

        if (prevVel == null) return;

        double diff = t.velocity - prevVel;

        // 급가속 감지
        if (diff >= 20) {
            if (Boolean.TRUE.equals(wasRapidAccel.value())) return; // 이전에도 급가속이면 리턴
            out.collect(getEventDTO(EventType.RAPID_ACCELERATION, t, time).toString());
            wasRapidAccel.update(true);      // 급가속 상태로 전환
            wasRapidDecel.update(false);     // 급감속 상태 해제
            return;
        }

        // 급감속 감지
        if (diff <= -20) {
            if (Boolean.TRUE.equals(wasRapidDecel.value())) return;
            out.collect(getEventDTO(EventType.RAPID_DECELERATION, t, time).toString());
            wasRapidDecel.update(true);
            wasRapidAccel.update(false);
            return;
        }

        // 변화가 없을 경우 상태 초기화
        wasRapidAccel.update(false);
        wasRapidDecel.update(false);
    }

    private void detectOverspeed(Telemetry t, LocalDateTime time, Collector<String> out) throws Exception {
        Boolean prev = wasOverspeed.value();
        boolean isOverspeed = t.velocity >= 50;

        // 현재도 과속이고 이전도 과속이면 중복 방지
        if (Boolean.TRUE.equals(prev) && isOverspeed) return;

        // 과속이 아니라면 상태만 업데이트하고 리턴
        if (!isOverspeed) {wasOverspeed.update(false);return;}

        // 여기 도달하면 이전은 과속 아니었고, 현재는 과속
        out.collect(getEventDTO(EventType.OVERSPEED, t, time).toString());
        wasOverspeed.update(true);
    }


    // 차선 침범
    private void detectInvasion(Telemetry t, LocalDateTime time, Collector<String> out) throws Exception {
        // 침범 상태가 아님 → 상태 업데이트 후 리턴
        if (t.invasion == null || t.invasion.isEmpty()) {lastInvasionState.update(false);return;}

        Boolean wasInvasion = lastInvasionState.value();

        // 이미 침범 상태였으면 이벤트 발생시키지 않음
        if (Boolean.TRUE.equals(wasInvasion)) return;

        // 침범 최초 감지 시 이벤트 발생
        out.collect(getEventDTO(EventType.INVASION, t, time).toString());
        lastInvasionState.update(true);
    }


    private void detectCollision(Telemetry t, LocalDateTime time, Collector<String> out) throws Exception {
        boolean isCollision = t.collision != null && !t.collision.isEmpty();

        Boolean was = wasCollision.value();

        if (!isCollision) {wasCollision.update(false);return;}

        if (Boolean.TRUE.equals(was)) return;

        out.collect(getEventDTO(EventType.COLLISION, t, time).toString());
        wasCollision.update(true);
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
