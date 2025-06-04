package com.amazonaws.services.msf.util;

import com.amazonaws.services.msf.model.Telemetry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.time.ZoneOffset;

public final class WatermarkFactory {
    public static WatermarkStrategy<Telemetry> telemetry() {
        return WatermarkStrategy
                .<Telemetry>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((t,ts)->t.time.toInstant(ZoneOffset.UTC).toEpochMilli());
    }
}