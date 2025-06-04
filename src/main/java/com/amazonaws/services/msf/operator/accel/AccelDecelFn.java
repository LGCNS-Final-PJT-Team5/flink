package com.amazonaws.services.msf.operator.accel;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import com.amazonaws.services.msf.util.EventFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AccelDecelFn extends RichFlatMapFunction<Telemetry, Event> {

    private transient ValueState<Double> lastVel;

    @Override
    public void open(Configuration parameters) {
        lastVel = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("lastVelocity", Double.class));
    }

    @Override
    public void flatMap(Telemetry t, Collector<Event> out) throws Exception {
        Double prev = lastVel.value();
        lastVel.update(t.velocity);

        if (prev == null) return;

        double diff = t.velocity - prev;

        if (diff >= 10) {
            out.collect(EventFactory.from(t, EventType.RAPID_ACCELERATION));
        }
        if (diff <= -10) {
            out.collect(EventFactory.from(t, EventType.RAPID_DECELERATION));
        }
    }
}
