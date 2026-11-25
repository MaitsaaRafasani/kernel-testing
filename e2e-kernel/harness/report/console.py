"""Console output: PASS / FAIL / WARN / SKIP per scenario x family x step."""


def scenario_start(usecase: str, scenario_id: str, family: str, imei: str, model: str) -> None:
    print(f"\n=== {usecase} / {scenario_id} / {family} (imei {imei}, model {model})")


def sent(label: str, at, subject: str, packet: str) -> None:
    print(f"  [SENT] {label:<22} at={at.isoformat()} subject={subject} data={packet}")


def info(message: str) -> None:
    print(f"  [INFO] {message}")


def warn(message: str) -> None:
    print(f"  [WARN] {message}")


def fail(message: str) -> None:
    print(f"  [FAIL] {message}")


def check(step_name: str, ok: bool, message: str) -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {step_name}: {message}")


def skip(message: str) -> None:
    print(f"  [SKIP] {message}")


def summary(total: int, failed: int, skipped: int = 0) -> None:
    print(f"\n{total - failed - skipped}/{total} scenario x family OK, {failed} gagal, {skipped} dilewati (belum siap)")
