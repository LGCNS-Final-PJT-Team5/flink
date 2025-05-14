package com.amazonaws.services.msf.dto;

import com.amazonaws.services.msf.event.EventType;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;

import java.time.LocalDateTime;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Event {
    public Long userId;           // 없으면 0L 등 기본값
    public String type;             // EventType의 코드값
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    public LocalDateTime time;    // ISO-8601
    public double gnssX;           // "37.1234,127.5678"
    public double gnssY;           // "37.1234,127.5678"
    public double driveId;        // Telemetry.driveId
}
