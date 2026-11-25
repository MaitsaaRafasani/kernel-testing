# e2e-kernel

End-to-end tests for kernel v2. Packets are built like a real device, published to NATS like iot-gateway,
processed by the kernel, and the results are checked automatically in the DB.

## Setup

```bash
pip install -r requirements.txt
```

| File | Content | Git |
|---|---|---|
| `.env` | Connections and credentials (NATS, DB, gate IP) | No |
| `env.yaml` | Test settings: test devices, timeouts, flags, field defaults | Yes |


### Before a run

- The test device (IMEI in `env.yaml` `devices`, or `imei` in the case) is registered in the DB
  (`device` → `vehicle` → `vehicle_status`) and reset with the kernel API `reset_vehicle_scenario/{imei}`.
- Every requirement in the case's `db_requirements` is prepared (sensors + pins, company settings, ...).
- The kernel runs with `OTEL_DEPLOYMENT_ENVIRONMENT=staging`.
- Test company `park_timeout` ≤ 10 minutes (so trips end).
- Kernel workers must not restart during a run.
- Each case needs its own vehicle (pins differ and every run needs a reset).

## Running

```bash
python cli.py ping                                   # check NATS (+ stream) and DB
python cli.py run --dry-run --usecase door           # build and print packets, nothing is sent
python cli.py run --usecase door,door_concox         # send + check expected
```

| Command | Purpose |
|---|---|
| `run` | All usecases, gate v2, expected v2 |
| `run --usecase a,b --family concox,teltonika` | Select usecases / families (comma separated, empty = all) |
| `run --imei 358000000000123` | Override the IMEI (only with 1 `--family`) |
| `run --no-check` | Send only, no expected check |
| `run --dry-run` | Print packets, no send / DB check |
| `run --keep-going` | A failed check does not stop the scenario: later steps are still sent and checked |
| `run --expect v3` | Check another expected version |
| `run --include-slow` | Also run scenarios tagged `slow` |
| `generate --usecase x` | Write case + expected from `generators/x.py` |
| `route-fetch --name x --points "lng,lat;lng,lat" --label "A -> B"` | Real road route from OSRM → `routes/x.json` |
| `yaml2excel --usecase x [--force]` | Build `cases/excel/x.xlsx` from YAML for editing |
| `excel2yaml [--usecase x]` | Save the Excel edits back to YAML |
| `import-scenario --file f.txt --usecase x` | Kernel scenario file → timeline YAML |
| `format [--usecase x] [--check]` | Rewrite YAML in the standard format (values unchanged). `--check` = check only |

### How a run works

Per usecase × family:

1. **Preflight** (read-only, 1 query). Fails → `[SKIP]`.
   - IMEI exists in `device`, `vehicle`, `vehicle_status`.
   - Sensor `speed` + the sensors in the case `setup` exist with the pin the family creator uses, and the pin is
     not used by another sensor.
   - `vehicle_status` is still reset. If not: SKIP, or WARN and run when `preflight.force: true`.
2. **Send** each step to NATS `PACKET.<imei>`.
3. **Check expected** of that step: the query repeats until it matches or `timeout.step` passes.
   - Checks the newest row matching `where`. Old runs are filtered by the time rules in `versions/v2/tables.yaml`.
   - A failed check stops the scenario after that step (unless `--keep-going`). Other families keep running.

Output is printed to the terminal. Any FAIL or SKIP → exit code 1.

```
=== device_normal / device_normal / concox (imei 358000000000001, model gt06n)
  [INFO] device siap: vehicle_id 12, sensor speed
  [SENT] step 5 heartbeat ...
  [PASS] heartbeat: vehicle_status [vehicle_id=12] battery, signal_strength
  [FAIL] park_end: summary_trip [vehicle_id=12] count: expected 1, actual 0
```

## Project structure

| Path | Content |
|---|---|
| `cli.py` | All commands |
| `env.yaml` | Test settings |
| `cases/<usecase>.yaml` | Case: settings, `db_requirements`, timeline |
| `versions/v2/expected/<usecase>.yaml` | Expected v2 |
| `versions/v2/` | Kernel v2 specifics: gate format, table time rules, preflight |
| `generators/` | Cases built by code (`build() -> (case, expected)`). `*_common.py` = shared simulations of kernel logic |
| `routes/` | Real road geometry (GeoJSON from OSRM) |
| `creators/` | Field → hex per family (Teltonika, Concox, Topflytech, Ruptela, Queclink). Fields: `creators/fields.yaml` |
| `harness/` | Runner, timeline, expected matcher, Excel / YAML format, routes |
| `service/` | NATS and DB connections |
| `tests/` | Unit tests |

## Editing test cases

YAML is the source of truth in git. Pick the path by where the case comes from (the YAML header says which):

| Case source | How to edit |
|---|---|
| `generators/<usecase>.py` exists | Edit the generator, then `python cli.py generate --usecase <usecase>`. Never edit the YAML by hand: it is overwritten (`excel2yaml` refuses it) |
| Plain YAML | `yaml2excel` → edit in Excel, save and **close** → `excel2yaml` → delete the xlsx. Small changes can go directly in YAML, then `format` |
| Kernel scenario file | `import-scenario` (overwrites the timeline, keeps other keys) |

After any change:

```bash
python cli.py format --check
python cli.py run --dry-run --usecase <usecase>
python -m pytest -q
```

### Case settings

| Key | Content |
|---|---|
| `families` | Families that run the case |
| `start` | `now-00:35:00`, `today-2d` (local midnight, 2 days ago = `H1 00:00:00`) |
| `interval` | Default gap of repeated rows |
| `imei`, `models` | Override per family |
| `tags` | `slow` = only with `--include-slow` |
| `carry_over` | `false` = an empty cell means the device does not send the value |
| `realtime` | `true` = send each packet at its real time (for logic using server time) |
| `received_on` | `send` (default) = gate time of sending. `packet` = packet time, needed when the kernel takes the time from `received_on` |
| `setup` | Required sensors: `{kind: sensors, code: door1, pin: digital_2}` |


### Timeline

| Column | Content |
|---|---|
| `from` | `HH:MM:SS` from `start` |
| `to`, `every` | Repeated row: a packet every `every` until before `to` (empty `every` = `interval`) |
| `step_name` | Step name used by expected (repeated row: last packet). `first_step_name` = first packet |
| `packet` | `login`, `location`, `heartbeat`, `alarm`, `sensor`, `command_reply`, `connect` / `disconnect`, `xmit` |
| `families` | Empty = all families |
| `batch`, `sent_at` | Buffered data: rows with the same `batch` go in 1 message, sent at `sent_at` |
| Other columns | Fields: `lat`, `lng`, `speed`, `acc`, `door1`, `battery`, `reply_text`, ... |

- Empty cell = same as the previous row (except per-step options like `reply_text`, `port_gate`).
- `106.81 -> 106.83` = interpolate across a repeated row. Only for a fixed position, never for driving.
- Same time → row order.
- Location gap < 15 minutes. The last packet must be in the past at run time.

### Expected

1 line = 1 check after `step_name` is sent: `table, where, count, match, timeout, known_bug, note`.

```yaml
- {table: summary_device_error, where: {vehicle_id: $vehicle_id, error_type: DOOR-01, start_time: 'H1 01:29:50', end_time: 'H1 02:00:00'}, count: 1}
```

- Variables: `$step_at`, `$at.<step>`, `$imei`, `$vehicle_id`, `$company_id`, values sent in the step
  (`$ip_gate`, `$port_gate`, `$reply_text`), `H2 00:30:00` (timeline time, `H1 00:00:00` = `start`).
- Values: exact, `~ 106.83` (approximate), `~ 7920 ± 150`. Times in `where` match ±1 second.
- `timeout`: max wait for that check (default `timeout.step`).
- `known_bug: <reason>`: checks the correct behaviour of a known kernel bug. It fails until the kernel is fixed
  (`[FAIL] [BUG KERNEL: ...]`) but does not stop the scenario.

## Notes for editing code

- **Moving vehicles follow real roads.** Fetch a route with `route-fetch` and drive it with `harness/route.py`
  (`Route`, `drive`, `location_row`, `check_moves`, `mileage`). Straight lines are only for parked positions.
  Example: `generators/device_normal.py`.
- **Expected comes from simulating the kernel.** `generators/*_common.py` copy kernel logic (device health, door,
  temperature). When the kernel logic changes, update the simulation and regenerate the cases that use it.
- **Use `known_bug`** for a kernel bug instead of expecting the wrong result.
- **One YAML format.** `excel2yaml`, `generate`, `import-scenario` and `format` all write through
  `harness/excel.py` `yaml_text`. `tests/test_yaml_format.py` fails when a file differs.
- **New family field:** add it to the family creator and `creators/fields.yaml`.
- **Kernel v2 specifics** (gate format, table time rules, preflight) live in `versions/v2/`. A new kernel version
  gets its own folder.
- Do not commit `.env` or `cases/excel/` (both git-ignored).

## Unit tests

```bash
python -m pytest -q
```

The creator round-trip tests need `KERNEL_V2_PATH` (path to the kernel v2 repo); without it they are skipped.
