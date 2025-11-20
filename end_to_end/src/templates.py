from decouple import config
from datetime import datetime, timedelta, timezone

vars = {
    'imei': config('IMEI_0', default='123000000000001', cast=str),
    'imei_hex': config('IMEI_0', default='123000000000001', cast=str).encode().hex(),
    'imei_1': config('IMEI_1', default='123000000000001', cast=str),
    'imei_1_hex': config('IMEI_1', default='123000000000001', cast=str).encode().hex(),
    'imei_trackvision': config('IMEI_TV', default='123000000000002', cast=str),
    'target': 1,
    'target_indo': 4,
    'target_trackvision': 2,
    'target_trackvision_indo': 8,
    'target_nsq_debug': 1,
    'ip_gate': config('TCP_HOST', default='localhost'),
    'port_gate': config('TCP_PORT', default=1200),
    'pod_ip_0': config('TCP_HOST_0', default='localhost'),
    'port_0': config('TCP_PORT_0', default=1200),
    'pod_ip_1': config('TCP_HOST_1', default='localhost'),
    'port_1': config('TCP_PORT_1', default=1200),
    'date': ((datetime.now(timezone(timedelta(hours=7))) + timedelta(days=30)).astimezone(timezone.utc)).isoformat(),
    'date_now': (datetime.now(timezone(timedelta(hours=7))).astimezone(timezone.utc)).isoformat()
}

# TEMPLATE DATA
def nsq_data(imei=None, raw_data=None, model=None, family=None):
    return {
        "data": raw_data,
        "identifier": imei,
        "family": family,
        "model": model,
        "received_on": ""
    }

def nsq_connection_info_connect(imei):
    return {
        "message_type": "connection_status",
        "identifier": imei,
        "message": {
            "status": "connected",
            "ip_gate": '',
            "port_gate": vars["port_gate"],
            "gate_start_time": ""
        },
        "time": ""
    }

def nsq_connection_info_disconnect(imei):
    return {
        "message_type": "connection_status",
        "identifier": imei,
        "message": {
            "status": "disconnected",
            "last_connect": ""
        },
        "time": ""
    }

def db_check_device(imei, family, model, target, connection_status, connection_status_time):
    return {
        "query": "SELECT imei, family, model, target, connection_status, connection_status_time FROM devices WHERE imei = %s",
        "params": [imei],
        "assertions": {
            "imei": imei,
            "family": family,
            "model": model,
            "target": target,
            "connection_status": connection_status,
            "connection_status_time": connection_status_time,
        },
    }

def db_check_device_iccid(imei, iccid, iccid_last_update):
    return {
        "query": "SELECT imei, iccid, iccid_last_update FROM devices WHERE imei = %s",
        "params": [imei],
        "assertions": {
            "imei": imei,
            "iccid": iccid,
            "iccid_last_update": iccid_last_update
        },
    }

def db_check_command_queue(imei, asserttion=None, command=None, expired_on=None):
    return {
        "query": "SELECT imei, command, expired_on FROM command_queue WHERE imei = %s",
        "params": [imei],
        "assertions": {
            "imei": imei,
            "command": command,
            "expired_on": expired_on
        } if asserttion else []
    }

def to_hex_string(str):
    return str.encode().hex()

# PRE-TEST
def delete_device(imei):
    return f"DELETE FROM devices WHERE imei = '{imei}'"

def update_ip_port(imei, host, port):
    return f"UPDATE devices SET pod_ip = '{host}:{port}' WHERE imei = '{imei}'"

def insert_device(imei, family, model, target):
    def quote(val):
        if val in (None, 'None', 'null'):
            return 'null'
        return f"'{val}'"

    family = quote(family)
    model = quote(model)
    target = 'null' if target in (None, 'None', 'null') else str(target)

    return f"""
        INSERT INTO devices (imei, family, model, target)
        VALUES ('{imei}', {family}, {model}, {target})
        ON CONFLICT (imei) DO UPDATE
        SET family = {family}, model = {model}, target = {target}, connection_status = false, connection_status_time = Null
    """

def reset_command_queue(imei):
    return f"DELETE FROM command_queue WHERE imei = '{imei}'"
