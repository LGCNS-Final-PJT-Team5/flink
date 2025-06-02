package com.amazonaws.services.msf.operator.sharpturn;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SharpTurnFn extends RichFlatMapFunction<Telemetry, Event> {

    private static final double MIN_SPEED = 30.0;
    private static final double STEER_THRESHOLD = 0.6;

    private transient ValueState<Boolean> wasSharp;

    @Override
    public void open(Configuration parameters) {
        wasSharp = getRuntimeContext().getState(new ValueStateDescriptor<>("wasSharp", Boolean.class));
    }

    @Override
    public void flatMap(Telemetry t, Collector<Event> out) throws Exception {
        boolean sharp = t.velocity > MIN_SPEED && Math.abs(t.Steer) > STEER_THRESHOLD;

        Boolean prev = wasSharp.value();
        wasSharp.update(sharp);

        if (!sharp || Boolean.TRUE.equals(prev)) {return;}

        out.collect(Event.builder()
                .userId(t.userId)
                .type(EventType.SHARP_TURN.toString())
                .time(t.time)
                .build());
    }
}
