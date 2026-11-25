"""Teltonika IO elements read by kernel v2 parser (family/teltonika/teltonika.py).

Name = TeltonikaParserCode name in lowercase. Size = byte section the parser reads it from.
Same id can appear with different names (parser picks by device model).
"""

X_BYTE = "x"

_SECTIONS = {
    1: {
        "immobilizer": 0xF8, "battery_percentage": 0x71,
        "digital_input_1": 0x01, "digital_input_2": 0x02, "digital_input_3": 0x03, "digital_input_4": 0x04,
        "gsm_signal_strength": 0x15, "profile_id": 0x16, "movement_event": 0x0136, "movement_sensor": 0xF0,
        "power_event": 0x0186, "button_click_event": 0x0185, "person_fall_event": 0xF2, "amber_event": 0x0190,
        "charge_status": 0x74, "ignition_id": 0xEF, "axl_callibration_status": 0x017F,
        "digital_output_1": 0xB3, "digital_output_2": 0xB4, "digital_output_3": 0x017C,
        "digital_output_3_1": 0x32, "digital_output_4_1": 0x33,
        "driving_type": 0xFD, "vehicle_battery_level": 0x98, "vehicle_battery_charging_status": 0x0393,
        "over_speed": 0xFF, "bluetooth_status": 0x0107, "gsm_jamming": 0xF9, "gps_jamming": 0x013E,
        "crash_detected": 0xF7, "sleep_mode": 0xC8, "gnss": 0x45,
        "ble_battery_1": 0x1D, "ble_battery_2": 0x14, "ble_battery_3": 0x1B,
        "eye_humidity_1": 0x2A34, "eye_humidity_2": 0x2A35, "eye_humidity_3": 0x2A36,
        "eye_magnet_1": 0x2A38, "eye_magnet_2": 0x2A39, "eye_magnet_3": 0x2A3A,
        "eye_battery_1": 0x2A44, "eye_battery_2": 0x2A45, "eye_battery_3": 0x2A46,
        "dtc_number": 0x1E, "engine_load": 0x1F, "collant_temperature": 0x20, "short_fuel_trim": 0x21,
        "intake_map": 0x23, "vehicle_speed": 0x25, "timing_advance": 0x26, "intake_air_temperature": 0x27,
        "throttle_position": 0x29, "commanded_egr": 0x2E, "egr_error": 0x2F, "fuel_level_obd": 0x30,
        "barometric_pressure": 0x32, "ambient_air_temperature": 0x35, "hybrid_battery_pack_life": 0x39,
        "engine_oil_temperature": 0x3A, "fuel_type": 0x02F7,
        "oem_battery_charge_status": 0x019A, "oem_battery_charge_level": 0x019B,
        "fuel_level_percent": 0x59, "throttle_position_can": 0x52, "speed_can": 0x51, "engine_torque": 0x72,
        "hand_brake_state": 0x028D, "pedal_brake_state": 0x038E, "pedal_clutch_state": 0x038F,
        "door_status_front_left": 0x028E, "door_status_front_right": 0x028F,
        "door_status_rear_left": 0x0290, "door_status_rear_right": 0x0291,
        "door_status_trunk": 0x0292, "door_status_hood": 0x0391,
    },
    2: {
        "analog_input_1": 0x09, "analog_input_2": 0x06, "analog_input_2_2": 0x0A, "analog_input_3": 0x0B,
        "vehicle_battery_voltage": 0x0458, "vehicle_battery_power": 0x0485, "vehicle_battery_current": 0x0459,
        "gsm_cell_id": 0xCD, "gsm_area_code": 0xCE, "gnss_hdop": 0xB6, "gnss_pdop": 0xB5,
        "external_voltage": 0x42, "last_known_position": 0x0182,
        "x_acc": 0x11, "y_acc": 0x12, "z_acc": 0x13,
        "battery_voltage": 0x43, "battery_current": 0x44,
        "ble_temperature_1": 0x19, "ble_temperature_2": 0x1A, "ble_temperature_3": 0x1B,
        "eye_temperature_1": 0x2A30, "eye_temperature_2": 0x2A31, "eye_temperature_3": 0x2A32,
        "eye_battery_voltage_1": 0x2A48, "eye_battery_voltage_2": 0x2A49, "eye_battery_voltage_3": 0x2A4A,
        "ble_humidity_1": 0x56, "ultrasonic_fuel": 0x0147,
        "lls_fuel_1": 0xC9, "lls_fuel_2": 0xCB, "lls_fuel_3": 0xD2, "lls_fuel_4": 0xD4,
        "ble_fuel_level_1": 0x010E, "fuel_pressure": 0x22, "engine_rpm": 0x24, "maf": 0x28,
        "runtime_engine_start": 0x2A, "distance_traveled_mil_on": 0x2B,
        "relative_fuel_rail_pressure": 0x2C, "direct_fuel_rail_pressure": 0x2D,
        "distance_since_codes_clear": 0x31, "control_module_voltage": 0x33, "absolute_load_value": 0x34,
        "fuel_injection_timing": 0x3B, "hybrid_system_voltage": 0x021F, "hybrid_system_current": 0x0220,
        "fuel_rate": 0x3C, "oem_battery_power_consumption": 0x019C, "oem_remaining_distance": 0x02F3,
        "fuel_level_liter": 0x54, "door_status": 0x5A, "engine_rpm_can": 0x55, "engine_temperature": 0x73,
    },
    4: {
        "accelerometer_data": 0x17, "vehicle_battery_energy_consumed": 0x0483,
        "vehicle_battery_energy_recuperated": 0x0484, "gsm_operator_data": 0xF1,
        "trip_odometer": 0xC7, "total_odometer": 0x10,
        "dallas_temperature_1": 0x48, "dallas_temperature_2": 0x49,
        "dallas_temperature_2_2": 0x4A, "dallas_temperature_3": 0x4A,
        "extended_external_voltage": 0x0320, "total_engine_duration": 0x01C1,
        "oem_total_mileage": 0x0185, "oem_fuel_level": 0x0186,
        "ble1_custom_2": 0x01CF, "ble1_custom_3": 0x01D0, "ble1_custom_4": 0x01D1, "ble1_custom_5": 0x01D2,
        "total_mileage": 0x57, "fuel_consumption": 0x53, "fuel_consumed_counted": 0x6B,
        "motor_hours": 0x66, "motor_hours_counted": 0x67, "remaining_distance": 0x0362,
    },
    8: {
        "i_button": 0x4E, "rfid": 0xCF, "eco_maximum": 0x0102,
        "dallas_temperature_1_id": 0x4C, "dallas_temperature_2_id": 0x4D, "dallas_temperature_3_id": 0x4E,
        "dallas_temperature_1_id_2": 0x3E, "dallas_temperature_2_id_2": 0x3F, "dallas_temperature_3_id_2": 0x40,
        "dallas_temperature_1_id_3": 0x49, "dallas_temperature_2_id_3": 0x4B,
        "dallas_temperature_1_id_4": 0x4B, "dallas_temperature_2_id_4": 0x4C,
    },
    X_BYTE: {
        "ble1_custom_1": 0x014B, "ble2_custom_1": 0x014C, "ble3_custom_1": 0x014D,
        "vin_number": 0x0100, "fault_codes": 0x0119, "beacon": 0x0181,
        "eye_beacon_visible": 0x2A4C, "eye_beacon_found": 0x2A4D, "eye_beacon_lost": 0x2A4F,
    },
}

# 2-byte variant of dallas temperature (model specific)
_SECTIONS[2].update({
    "dallas_temperature_1_2byte": 0x48, "dallas_temperature_2_2byte": 0x49, "dallas_temperature_3_2byte": 0x4A,
})

# Manual CAN, 8 bytes (parser MANUAL_CAN_KEYS). Zero payload is ignored by the parser.
_SECTIONS[8].update({f"manual_can_{n}": 0x0384 + n for n in range(30)})                  # TFT100
_SECTIONS[8].update({f"manual_can_ft_{n}": 0x27E8 + n for n in range(20)})               # FTC305 0-19
_SECTIONS[8].update({f"manual_can_ft_{n}": 0x283A + n - 20 for n in range(20, 70)})      # FTC305 20-69

# name -> (io id, size in bytes or X_BYTE)
IO: dict[str, tuple[int, int | str]] = {
    name: (io_id, size) for size, items in _SECTIONS.items() for name, io_id in items.items()
}

EYE_BEACON_IO = {"eye_beacon_visible", "eye_beacon_found", "eye_beacon_lost"}
