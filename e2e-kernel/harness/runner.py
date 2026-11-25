"""Run scenario x family: send the timeline to NATS and check expected rows in DB per step.

packet:
- login / location / sensor / heartbeat / alarm / command_reply -> Family Creator -> gate message `data`
- connect / disconnect / xmit                                   -> gate message directly (family independent)

Before sending, versions/<expect>/setup.py checks the device is registered and reset (skip if not ready).
After a gate message is published, every named step in it (step_name / first_step_name) that has checks
in versions/<expect>/expected/<usecase>.yaml is checked in DB. A failed check stops that scenario x family
(next steps depend on it). One failing scenario x family never stops the others.
A check with `known_bug: <reason>` expects the correct behaviour of a known kernel bug: it fails until the kernel is
fixed, is reported as FAIL with the reason, but does not stop the scenario.
"""
import asyncio
import importlib
import random
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from creators.registry import get_creator
from harness.config import ROOT, load_yaml
from harness.db_poller import check_db
from harness.expected import CheckContext, load_expected, resolve, steps_for
from harness.identifiers import resolve_imei, resolve_model, validate_imei
from harness.injector import Injector, gate_time
from harness.loader import load_cases, load_gate_format
from harness.report import console
from harness.timeline import Step, expand_steps, initial_fields, parse_duration, parse_time
from service.db import Database
from service.nats import NatsClient

GATE_MESSAGES = ("connect", "disconnect", "xmit")
XMIT_PREFIX = "7000,EC,"
RANDOM_PORT_MIN, RANDOM_PORT_MAX = 20000, 60000
ON_CHECK_FAIL = ("continue", "stop")
RECEIVED_ON = ("send", "packet")      # case `received_on`: moment e2e sends / packet send moment on the timeline
OK, FAIL, SKIP = "ok", "fail", "skip"
MAX_SENT_LINES = 300          # more gate messages than this: [SENT] only for named steps + progress
PROGRESS_EVERY = 1000
LONG_WAIT_SECONDS = 600       # realtime: log waits longer than this


@dataclass
class Preflight:
    db: Database
    setup: object            # module versions/<expect>/setup.py
    force: bool              # run even if vehicle_status is not reset


@dataclass
class StepResult:
    ok: bool
    done: int                    # checks run
    passed: int
    known_bug_only: bool         # every failed check is a `known_bug` check


@dataclass
class Checker:
    db: Database
    tables: dict
    expected: dict
    timeout: timedelta
    poll: timedelta
    clock_skew: timedelta
    approx_tolerance: float
    stop_on_check_fail: bool
    keep_going: bool = False     # --keep-going: a step with a failed check does not stop the scenario


async def run(
    config: dict, envelope: str, expect: str, usecases: list[str] | None, families: list[str] | None,
    include_slow: bool = False, cli_imei: str | None = None, dry_run: bool = False, no_check: bool = False,
    keep_going: bool = False,
) -> int:
    """Return exit code (0 = all OK; failed or skipped = 1)."""
    cases = load_cases(usecases)
    checking = not dry_run and not no_check
    client = db = preflight = None
    try:
        if not dry_run:
            client = NatsClient(config["nats"])
            await client.connect()
            if not await client.stream_exists(config["nats"]["stream"]):
                console.fail(f"NATS stream '{config['nats']['stream']}' tidak ada di {client.server_url}")
                return 1
            db = Database(config["db"])         # preflight always, expected checks unless --no-check
            await db.connect()
            force = str((config.get("preflight") or {}).get("force", False)).lower() == "true"
            preflight = Preflight(db, importlib.import_module(f"versions.{expect}.setup"), force)
        injector = Injector(config["nats"], load_gate_format(envelope), client)

        total = failed = skipped = 0
        for case in cases:
            checker = make_checker(config, db, expect, case["usecase"], keep_going) if checking else None
            if checking and checker is None:
                console.warn(f"{case['usecase']}: belum ada expected {expect}, hanya kirim data")
            for scenario in case.get("scenarios") or []:
                if "slow" in (scenario.get("tags") or []) and not include_slow:
                    continue
                for family in scenario.get("families") or []:
                    if families and family not in families:
                        continue
                    total += 1
                    status = await run_scenario(
                        config, injector, preflight, checker, case["usecase"], scenario, family, cli_imei,
                    )
                    failed += status == FAIL
                    skipped += status == SKIP
    finally:
        if client is not None:
            await client.close()
        if db is not None:
            await db.close()

    console.summary(total, failed, skipped)
    return 1 if failed or skipped else 0


def make_checker(
    config: dict, db: Database, expect: str, usecase: str, keep_going: bool = False,
) -> Checker | None:
    expected = load_expected(expect, usecase)
    if expected is None:
        return None
    tables_path = ROOT / "versions" / expect / "tables.yaml"
    timeout = config.get("timeout") or {}
    match = config.get("match") or {}
    on_check_fail = match.get("on_check_fail", "continue")
    if on_check_fail not in ON_CHECK_FAIL:
        raise ValueError(f"env match.on_check_fail harus {' / '.join(ON_CHECK_FAIL)}, bukan '{on_check_fail}'")
    return Checker(
        db=db,
        tables=load_yaml(tables_path) if tables_path.exists() else {},
        expected=expected,
        timeout=parse_duration(timeout.get("step", "00:00:10")),
        poll=parse_duration(timeout.get("poll", "00:00:01")),
        clock_skew=parse_duration(timeout.get("clock_skew", "00:00:05")),
        approx_tolerance=float(match.get("approx_tolerance", 0.00001)),
        stop_on_check_fail=on_check_fail == "stop",
        keep_going=keep_going,
    )


async def run_scenario(
    config: dict, injector: Injector, preflight: Preflight | None, checker: Checker | None, usecase: str,
    scenario: dict, family: str, cli_imei: str | None,
) -> str:
    """Return OK / FAIL / SKIP (device not ready)."""
    try:
        creator = get_creator(family, config.get("device_tz", 7))
        model = resolve_model(scenario, family, config, creator.default_model)
        imei = resolve_imei(scenario, family, config, cli_imei)
        validate_imei(imei, creator.identifier_format, family)
        received_mode = scenario.get("received_on", RECEIVED_ON[0])
        if received_mode not in RECEIVED_ON:
            raise ValueError(f"received_on harus {' / '.join(RECEIVED_ON)}, bukan '{received_mode}'")
        now = datetime.now(timezone.utc)
        timing = config.get("scenario") or {}
        tz = timezone(timedelta(hours=config.get("device_tz", 7)))
        timeline_start = parse_time(scenario.get("start", timing.get("start", "now")), now, tz)
        steps = expand_steps(
            scenario, initial_fields(config, family, creator.supported_fields), now, timing, family, tz,
        )
        console.scenario_start(usecase, scenario["scenario"], family, imei, model)

        device = {"imei": imei}
        if preflight:
            ready, device = await check_ready(preflight, imei, scenario.get("setup"), creator.sensor_pins)
            if not ready:
                return SKIP

        step_checks, context = {}, None
        if checker:
            step_checks = steps_for(checker.expected, scenario["scenario"], family)
            all_names = {s.id for s in expand_steps(scenario, {}, now, timing, tz=tz) if s.id}
            for name in sorted(set(step_checks) - all_names):
                console.warn(f"expected step '{name}' tidak ada di timeline (cek ejaan step_name)")
            context = CheckContext(
                device=device, step_times={s.id: s.at for s in steps if s.id}, timeline_start=timeline_start,
            )

        # auto send protocol packets (ex: login) unless the case sends them itself
        sent_by_case = {step.packet for step in steps}
        if steps:
            received_on = received_time(received_mode, steps[0].at)
            for packet_type in (p for p in creator.initial_packets if p not in sent_by_case):
                for packet in creator.build(packet_type, {}, steps[0].at, imei):
                    values = data_values(imei, family, model, packet, received_on)
                    await publish(injector, "data", values, packet_type, steps[0].at)

        gate = config.get("gate") or {}
        last_connect = None
        passed = checked = 0
        failed_steps = []
        realtime = bool(scenario.get("realtime"))
        groups = group_batches(steps)
        show_all = len(groups) <= MAX_SENT_LINES
        if not show_all:
            console.info(f"{len(groups)} pesan: [SENT] hanya untuk step bernama, progres tiap {PROGRESS_EVERY}")
        if realtime and steps:
            console.info(f"realtime: tiap paket dikirim pada waktunya, selesai {steps[-1].send_at.isoformat()}")
        for number, group in enumerate(groups, start=1):
            step = group[0]
            if realtime:
                await wait_until(step.send_at or step.at)
            if not show_all and number % PROGRESS_EVERY == 0:
                console.info(f"terkirim {number}/{len(groups)} (waktu paket {step.at.isoformat()})")
            if step.wait:
                console.info(f"step {step.index}: wait {step.wait}")
                await asyncio.sleep(step.wait.total_seconds())

            sent_values = {item.id: dict(item.fields) for item in group if item.id}
            received_on = received_time(received_mode, step.send_at or step.at)
            if step.batch:
                # buffered data: all steps of the batch in one gate message
                if any(item.packet in GATE_MESSAGES for item in group):
                    raise ValueError(f"batch '{step.batch}': connect / disconnect / xmit cannot be batched")
                payloads = creator.build_batch_messages([(item.packet, item.fields, item.at) for item in group], imei)
                label = f"batch {step.batch} ({len(group)} step, s/d {group[-1].at:%H:%M:%S})"
                messages = [("data", data_values(imei, family, model, payload, received_on)) for payload in payloads]
            elif step.packet in GATE_MESSAGES:
                label = f"step {step.index} {step.id or step.packet}"
                values = gate_values(step, imei, gate, last_connect)
                if step.packet == "connect":
                    last_connect = values["time"]
                if step.id:
                    sent_values[step.id].update(values)          # ex: real port_gate when `random`
                messages = [(step.packet, values)]
            else:
                label = f"step {step.index} {step.id or step.packet}"
                packets = creator.build(step.packet, step.fields, step.at, imei)
                messages = [("data", data_values(imei, family, model, packet, received_on)) for packet in packets]

            sent_at = datetime.now(timezone.utc)
            show = show_all or any(item.id for item in group)
            for _ in range(2 if step.duplicate else 1):
                for message, values in messages:
                    await publish(injector, message, values, label, step.at, show)

            if context:
                context.step_values.update(sent_values)
            for item in group:
                if item.id not in step_checks:
                    continue
                result = await verify_step(checker, item.id, step_checks[item.id], context, sent_at, timeline_start)
                checked += result.done
                passed += result.passed
                if result.ok:
                    continue
                if result.known_bug_only:
                    failed_steps.append(item.id)
                    console.fail(f"step '{item.id}': cek known_bug gagal (bug kernel belum diperbaiki), lanjut")
                elif stop_after_failed_check(checker, item.id, failed_steps):
                    return FAIL

        if checker:
            console.info(f"cek DB: {passed}/{checked} OK")
        if failed_steps:
            console.fail(f"{len(failed_steps)} step dengan cek gagal: {', '.join(failed_steps)}")
            return FAIL
        return OK
    except Exception as error:
        console.fail(f"{usecase} / {scenario.get('scenario')} / {family}: {error}")
        traceback.print_exc()
        return FAIL


async def wait_until(moment: datetime) -> None:
    """Realtime mode: sleep until the packet send moment."""
    delay = (moment - datetime.now(timezone.utc)).total_seconds()
    if delay > LONG_WAIT_SECONDS:
        console.info(f"realtime: tunggu sampai {moment.isoformat()}")
    if delay > 0:
        await asyncio.sleep(delay)


async def check_ready(
    preflight: Preflight, imei: str, setup: dict | None, creator_pins: dict,
) -> tuple[bool, dict]:
    """Device registered (+ sensors on the expected pins) and reset? Return (ready, device variables).
    Pin = setup `pin` of the sensor, else the creator pin."""
    sensors = preflight.setup.required_sensors(setup)
    sensor_pins = {**creator_pins, **preflight.setup.setup_pins(setup)}
    state = await preflight.setup.check_device(preflight.db, imei, sensors, sensor_pins)
    for problem in state.problems:
        console.skip(problem)
    if state.problems:
        return False, state.variables
    if not state.is_reset:
        if not preflight.force:
            console.skip(f"vehicle_id {state.variables['vehicle_id']} belum di-reset (vehicle_status bukan sentinel)."
                         " Reset lewat API kernel, atau env preflight.force: true")
            return False, state.variables
        console.warn(f"vehicle_id {state.variables['vehicle_id']} belum di-reset, tetap jalan (preflight.force)")
    names = [f"{name} (pin {sensor_pins[name]})" if name in sensor_pins else name for name in sensors]
    console.info(f"device siap: vehicle_id {state.variables['vehicle_id']}, sensor {', '.join(names)}")
    return True, state.variables


def stop_after_failed_check(checker: Checker, step_name: str, failed_steps: list[str]) -> bool:
    """A step had a failed check: stop the scenario, or (--keep-going) note it and go on with the next steps."""
    failed_steps.append(step_name)
    if checker.keep_going:
        console.fail(f"step '{step_name}' cek gagal, lanjut (--keep-going)")
        return False
    console.fail(f"step '{step_name}' gagal, scenario dihentikan")
    return True


async def verify_step(
    checker: Checker, step_name: str, checks: dict, context: CheckContext, sent_at: datetime,
    timeline_start: datetime,
) -> StepResult:
    """Run checks of one step. A failed check stops the rest of this step only when env
    match.on_check_fail = stop (a failed `known_bug` check never stops it)."""
    for kind in set(checks) - {"db"}:
        console.warn(f"step '{step_name}': cek '{kind}' belum didukung, dilewati")
    step = StepResult(ok=True, done=0, passed=0, known_bug_only=True)
    for check in checks.get("db") or []:
        resolved = resolve({key: value for key, value in check.items() if key != "note"}, context, step_name)
        known_bug = resolved.pop("known_bug", None)
        timeout = parse_duration(resolved.pop("timeout")) if "timeout" in resolved else checker.timeout
        result = await check_db(
            checker.db, resolved, checker.tables.get(resolved["table"]) or {}, sent_at, timeline_start,
            timeout, checker.poll, checker.approx_tolerance, checker.clock_skew,
        )
        step.done += 1
        message = f"[BUG KERNEL: {known_bug}] {result.message}" if known_bug else result.message
        console.check(step_name, result.ok, message)
        if result.ok:
            step.passed += 1
            continue
        step.ok = False
        if not known_bug:
            step.known_bug_only = False
            if checker.stop_on_check_fail:
                break
    return step


def group_batches(steps: list[Step]) -> list[list[Step]]:
    """Consecutive steps with the same `batch` form one group; other steps are alone."""
    groups: list[list[Step]] = []
    for step in steps:
        if step.batch and groups and groups[-1][-1].batch == step.batch:
            groups[-1].append(step)
        else:
            groups.append([step])
    return groups


def received_time(mode: str, send_moment: datetime) -> str | None:
    """Gate received_on: None = now when published (send), the timeline send moment (packet)."""
    return gate_time(send_moment) if mode == "packet" else None


def data_values(imei: str, family: str, model: str, packet: str, received_on: str | None = None) -> dict:
    return {"imei": imei, "family": family, "model": model, "payload": packet,
            "received_on": received_on or gate_time()}


def gate_values(step: Step, imei: str, gate: dict, last_connect: str | None) -> dict:
    """Values for connect / disconnect / xmit. Step fields override env `gate:` defaults."""
    fields = step.fields
    time = gate_time(step.at)
    if step.packet == "connect":
        port_gate = fields.get("port_gate", gate.get("port_gate", 0))
        if port_gate == "random":       # unique per run, avoids passing on data from an older run
            port_gate = random.randint(RANDOM_PORT_MIN, RANDOM_PORT_MAX)
        return {
            "imei": imei, "time": time,
            "ip_gate": fields.get("ip_gate", gate.get("ip_gate", "127.0.0.1")),
            "port_gate": int(port_gate),
            "gate_start_time": fields.get("gate_start_time", gate_time(datetime.now(timezone.utc))),
        }
    if step.packet == "disconnect":
        return {"imei": imei, "time": time, "last_connect": fields.get("last_connect", last_connect or time)}

    # xmit: raw `command`, or `message` (plain text) -> 7000,EC,<hex>
    if "command" in fields:
        command = fields["command"]
    elif "message" in fields:
        command = XMIT_PREFIX + str(fields["message"]).encode().hex()
    else:
        raise ValueError(f"step {step.index}: xmit needs 'command' or 'message'")
    return {
        "command": command,
        "ip": fields.get("ip", gate.get("xmit_ip", "127.0.0.1")),
        "port": int(fields.get("port", gate.get("xmit_port", 0))),
    }


async def publish(injector: Injector, message: str, values: dict, label: str, at, show: bool = True) -> None:
    subject, body = await injector.send(message, values)
    if show:
        console.sent(label, at, subject, values["payload"] if message == "data" else body)
