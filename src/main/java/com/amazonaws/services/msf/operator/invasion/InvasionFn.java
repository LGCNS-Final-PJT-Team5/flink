package com.amazonaws.services.msf.operator.invasion;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class InvasionFn extends RichFlatMapFunction<Telemetry, Event> {

    private transient ValueState<Boolean> wasInvasion;

    @Override
    public void open(Configuration parameters) {
        wasInvasion = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("wasInvasion", Boolean.class));
    }

    @Override
    public void flatMap(Telemetry t, Collector<Event> out) throws Exception {
        boolean isInvasion = t.invasion != null && !t.invasion.isEmpty();
        Boolean prev = wasInvasion.value();
        wasInvasion.update(isInvasion);

        if (!isInvasion || Boolean.TRUE.equals(prev)) {return;}

        out.collect(Event.builder()
                .userId(t.userId)
                .type(EventType.INVASION.toString())
                .time(t.time)
                .build());
    }
}
