package com.amazonaws.services.msf.operator.reactiondelay;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import com.amazonaws.services.msf.util.EventFactory;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 앞차와 5 m 미만 + front_object 존재 → 다음 프레임에서 Brake==0 이면
 * ‘반응 속도 미흡(Reaction Delay)’ 이벤트 1회 송출.
 */
public class ReactionDelayFn extends KeyedProcessFunction<String, Telemetry, Event> {

    /** 직전 프레임이 “위험”이었는지 */
    private transient ValueState<Boolean> pendingDanger;
    /** 이미 ReactionDelay 이벤트를 보냈는지 */
    private transient ValueState<Boolean> issued;

    @Override
    public void open(Configuration parameters) {
        pendingDanger = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("pendingDanger", Boolean.class));
        issued = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("issued", Boolean.class));
    }

    @Override
    public void processElement(Telemetry t,
                               Context ctx,
                               Collector<Event> out) throws Exception {

        /* 0) 주행 중이 아니면 상태 초기화 후 종료 */
        if (t.velocity <= 0) {
            clearStates();
            return;
        }

        boolean dangerNow = t.frontDistance < 5.0 &&
                t.frontObject != null &&
                !t.frontObject.isEmpty();

        /* 1) 이미 이벤트를 발행한 중이라면… */
        if (Boolean.TRUE.equals(issued.value())) {

            /* 위험 해제 또는 제동 감지 시 상태 리셋 */
            if (!dangerNow || t.Brake > 0) {
                clearStates();
            }
            return;
        }

        /* 2) 직전 프레임이 위험이었고 이번 프레임 브레이크 0 → 이벤트 */
        if (Boolean.TRUE.equals(pendingDanger.value()) && t.Brake == 0) {
            out.collect(EventFactory.from(t, EventType.REACTION_DELAY));
            issued.update(true);            // 이후 프레임에서 중복 차단
            return;                       
        }

        /* 3) 여기까지 왔으면 이벤트 조건 미충족 → 위험 플래그 업데이트 */
        pendingDanger.update(dangerNow);
    }

    private void clearStates() throws Exception {
        pendingDanger.clear();
        issued.clear();
    }
}
