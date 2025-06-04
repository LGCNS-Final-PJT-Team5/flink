package com.amazonaws.services.msf.operator.collision;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import com.amazonaws.services.msf.util.EventFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class CollisionFn extends RichFlatMapFunction<Telemetry, Event> {

    private transient ValueState<Boolean> wasCollision;

    @Override
    public void open(Configuration parameters) {
        wasCollision = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("wasCollision", Boolean.class));
    }

    @Override
    public void flatMap(Telemetry t, Collector<Event> out) throws Exception {

        boolean isCollision = t.collision != null && !t.collision.isEmpty();
        Boolean was = wasCollision.value();

        if (!isCollision) {wasCollision.update(false);  return;}

        if (Boolean.TRUE.equals(was)) {return;}

        out.collect(EventFactory.from(t, EventType.COLLISION));

        wasCollision.update(true);
    }
}
