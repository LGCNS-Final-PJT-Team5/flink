package com.amazonaws.services.msf.event;

public enum EventType {

    // 완료
    IDLE_ENGINE("공회전"),               // A-1
    RAPID_ACCELERATION("급가속"),       // B-1
    RAPID_DECELERATION("급감속"),
    OVERSPEED("과속"),          // B-2
    INVASION("차선 이탈"),

    // TODO
    NO_OPERATION("미조작"),
    SHARP_TURN("급회전"),
    SAFE_DISTANCE_VIOLATION("안전 거리 미준수");
    private final String description;

    EventType(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return description;
    }
}