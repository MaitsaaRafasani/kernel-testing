"""Entry point: `python cli.py run` / `ping` / `check`."""
import asyncio
import sys
from pathlib import Path

import click

from harness.config import ROOT, load_config
from harness.excel import excel2yaml as convert_excel_to_yaml
from harness.excel import format_yaml, stale_excel
from harness.excel import yaml2excel as convert_yaml_to_excel
from harness.preflight import ping as ping_connections
from harness.generate import generate_case
from harness.route import Route, fetch_route
from harness.runner import run as run_scenarios
from harness.scenario_import import import_scenario as convert_scenario

if sys.platform == "win32":
    # psycopg async does not work with the default Proactor loop (same as kernel v2)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def split(value: str | None) -> list[str] | None:
    return [item.strip() for item in value.split(",") if item.strip()] if value else None


@click.group()
def cli():
    pass


@cli.command()
@click.option("--usecase", default=None, help="pisah koma, mis. door,location. Kosong = semua")
@click.option("--family", default=None, help="pisah koma, mis. concox,teltonika. Kosong = semua")
@click.option("--envelope", default="v2", help="versi format gate (versions/<versi>/gate_format.yaml)")
@click.option("--expect", default="v2", help="versi expected (versions/<versi>/expected/)")
@click.option("--imei", default=None, help="override IMEI (hanya kalau 1 family dipilih)")
@click.option("--include-slow", is_flag=True, help="ikut jalankan scenario tag slow")
@click.option("--dry-run", is_flag=True, help="bangun paket dan tampilkan, tanpa kirim ke NATS / cek DB")
@click.option("--no-check", is_flag=True, help="kirim ke NATS saja, tanpa cek expected ke DB")
@click.option("--keep-going", is_flag=True,
              help="cek expected gagal tidak menghentikan scenario: step berikutnya tetap dikirim dan dicek")
def run(usecase, family, envelope, expect, imei, include_slow, dry_run, no_check, keep_going):
    """Kirim timeline ke kernel lalu cek expected ke DB per step."""
    families = split(family)
    if imei and (not families or len(families) != 1):
        raise click.UsageError("--imei hanya bisa dipakai dengan 1 --family")
    stale = stale_excel()
    if stale:
        click.echo(f"[WARN] excel lebih baru dari yaml (belum excel2yaml): {', '.join(stale)}")
    config = load_config()
    sys.exit(asyncio.run(run_scenarios(
        config, envelope, expect, split(usecase), families, include_slow, imei, dry_run, no_check, keep_going,
    )))


@cli.command()
def ping():
    """Cek koneksi NATS (+ stream) dan Postgres."""
    sys.exit(asyncio.run(ping_connections(load_config())))


@cli.command()
@click.option("--usecase", default=None, help="pisah koma. Kosong = semua cases/excel/*.xlsx")
def excel2yaml(usecase):
    """Simpan editan Excel ke YAML (ditimpa). Setelah itu xlsx boleh dihapus."""
    for path in convert_excel_to_yaml(split(usecase)):
        click.echo(f"written {path.relative_to(ROOT)}")


@cli.command()
@click.option("--usecase", default=None, help="pisah koma. Kosong = semua cases/*.yaml")
@click.option("--force", is_flag=True, help="timpa xlsx yang sudah ada")
def yaml2excel(usecase, force):
    """Buat Excel dari YAML untuk diedit: cases/excel/<usecase>.xlsx (tidak ditimpa tanpa --force)."""
    try:
        written = convert_yaml_to_excel(split(usecase), force)
    except FileExistsError as error:
        raise click.ClickException(str(error))
    for path in written:
        click.echo(f"written {path.relative_to(ROOT)}")


@cli.command("format")
@click.option("--usecase", default=None, help="pisah koma. Kosong = semua cases/*.yaml")
@click.option("--check", "check_only", is_flag=True, help="hanya cek, tidak menulis (exit 1 kalau ada yang beda)")
def format_command(usecase, check_only):
    """Rapikan cases + expected ke format standar (1 baris = 1 paket / 1 cek). Nilai tidak berubah."""
    changed = format_yaml(split(usecase), write=not check_only)
    for path in changed:
        click.echo(f"{'beda format' if check_only else 'written'} {path.relative_to(ROOT)}")
    if not changed:
        click.echo("semua yaml sudah format standar")
    if check_only and changed:
        sys.exit(1)


@cli.command("import-scenario")
@click.option("--file", "path", required=True, type=click.Path(exists=True, dir_okay=False),
              help="file scenario kernel, mis. ../mceasy-cckernel/scenario/device_health_REFRI.txt")
@click.option("--usecase", required=True, help="case tujuan: cases/<usecase>.yaml (timeline ditimpa, key lain tetap)")
def import_scenario(path, usecase):
    """File scenario kernel (.txt) -> timeline YAML (carry_over: false). File .txt tidak disalin ke e2e."""
    target, packets, rows = convert_scenario(Path(path), usecase)
    click.echo(f"written {target.relative_to(ROOT)}: {packets} paket -> {rows} baris timeline")


@cli.command("route-fetch")
@click.option("--name", required=True, help="nama file: routes/<name>.json")
@click.option("--points", required=True, help="lng,lat;lng,lat[;...] awal, (via), tujuan")
@click.option("--label", required=True, help="keterangan rute, mis. 'Jakarta -> Karawang'")
def route_fetch(name, points, label):
    """Rute jalan asli dari OSRM (OpenStreetMap) -> routes/<name>.json. Hanya koordinat yang dikirim."""
    coordinates = [tuple(float(part) for part in point.split(",")) for point in points.split(";")]
    path = fetch_route(name, coordinates, label)
    route = Route(name)
    click.echo(f"written {path.relative_to(ROOT)}: {route.length / 1000:.1f} km, {len(route.points)} titik")


@cli.command()
@click.option("--usecase", required=True, help="generators/<usecase>.py")
def generate(usecase):
    """Tulis ulang case + expected v2 dari generators/<usecase>.py (mis. rute jalan dari routes/)."""
    for path in generate_case(usecase):
        click.echo(f"written {path.relative_to(ROOT)}")


@cli.command()
@click.option("--envelope", default="v2")
@click.option("--expect", default="v2")
def check(envelope, expect):
    """Validasi cases, expected, gate_format, field, dan IMEI tanpa kirim data."""
    raise NotImplementedError


if __name__ == "__main__":
    cli()
