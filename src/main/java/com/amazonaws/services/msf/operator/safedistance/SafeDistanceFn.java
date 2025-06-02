package com.amazonaws.services.msf.operator.safedistance;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SafeDistanceFn extends RichFlatMapFunction<Telemetry, Event> {

    private static final double MIN_SPEED = 50.0;

    private transient ValueState<Boolean> wasTooClose;

    @Override
    public void open(Configuration parameters) {
        wasTooClose = getRuntimeContext().getState(new ValueStateDescriptor<>("wasTooClose", Boolean.class));
    }

    @Override
    public void flatMap(Telemetry t, Collector<Event> out) throws Exception {
        boolean tooClose = t.velocity >= MIN_SPEED && t.frontDistance < t.velocity;

        Boolean prev = wasTooClose.value();
        wasTooClose.update(tooClose);

        if (!tooClose || Boolean.TRUE.equals(prev)) {return;}

        out.collect(Event.builder()
                .userId(t.userId)
                .type(EventType.SAFE_DISTANCE_VIOLATION.toString())
                .time(t.time)
                .build());
    }
}
