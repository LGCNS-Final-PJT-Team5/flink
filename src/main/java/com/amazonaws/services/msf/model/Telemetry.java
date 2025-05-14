package com.amazonaws.services.msf.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

@Getter
@NoArgsConstructor
@ToString
@Builder
@AllArgsConstructor
public class Telemetry {
    public double velocity;         // km/h 기준
    public double driveId;
    public double Throttle;
    public double Steer;            // -1.0 ~ 1.0 범위 → 각도(°) 변환은 EventDetector에서
    public double Brake;
    public JsonNode collision;
    public JsonNode invasion;
    @JsonProperty("Accelero_x") public double accelX;
    @JsonProperty("Accelero_y") public double accelY;
    @JsonProperty("Accelero_z") public double accelZ;
    @JsonProperty("Gyroscop_x") public double gyroX;
    @JsonProperty("Gyroscop_y") public double gyroY;
    @JsonProperty("Gyroscop_z") public double gyroZ;
    @JsonProperty("GNSS_x") public double gnssX;
    @JsonProperty("GNSS_y") public double gnssY;
    @JsonProperty("front_distance") public double frontDistance;
    @JsonProperty("front_object")  public String frontObject;
}
