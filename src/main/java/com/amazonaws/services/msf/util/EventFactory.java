package com.amazonaws.services.msf.util;

import com.amazonaws.services.msf.dto.Event;
import com.amazonaws.services.msf.event.EventType;
import com.amazonaws.services.msf.model.Telemetry;

public final class EventFactory {

    private EventFactory() {
    }


    public static Event from(Telemetry t, EventType type) {
        return Event.builder()
                .userId(t.userId)
                .driveId(t.driveId)
                .type(type.toString())
                .time(t.time)
                .gnssX(t.gnssX)
                .gnssY(t.gnssY)
                .build();
    }
}
