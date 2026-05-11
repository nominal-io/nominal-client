from __future__ import annotations

import csv
import logging
import struct
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def convert_ouster_dataset(
    dataset_dir: Path,
    *,
    output_dir: Path | None = None,
    apply_nav: bool = True,
    max_scans: int | None = None,
) -> list[Path]:
    """Convert an Ouster PCAP dataset directory into CSV point cloud files.

    Reads the data.yaml manifest, decodes each Ouster lidar PCAP using
    ouster-sdk, optionally applies navigation poses from buf3 nav data,
    and writes one CSV per sensor with columns:
    x, y, z, time, reflectivity, signal, near_ir.

    Args:
        dataset_dir: Path to dataset directory containing data.yaml.
        output_dir: Directory for output CSVs. Defaults to dataset_dir.
        apply_nav: Whether to apply nav pose corrections. Default True.
        max_scans: Limit number of scans per sensor (useful for testing).

    Returns:
        List of paths to generated CSV files, one per sensor.

    Raises:
        FileNotFoundError: If data.yaml is not found in dataset_dir.
        ImportError: If ouster-sdk or pyyaml is not installed.
    """
    try:
        import yaml
    except ImportError:
        raise ImportError("pyyaml is required for Ouster conversion: pip install nominal[ouster]")

    if output_dir is None:
        output_dir = dataset_dir

    data_yaml = dataset_dir / "data.yaml"
    if not data_yaml.exists():
        raise FileNotFoundError(f"No data.yaml found in {dataset_dir}")

    with open(data_yaml) as f:
        manifest = yaml.safe_load(f)

    ouster_daqs = manifest.get("ousterDaqs", [])
    if not ouster_daqs:
        raise ValueError(f"No ousterDaqs entries in {data_yaml}")

    logger.info(
        "Dataset: %s | Location: %s | Vehicle: %s-%s | Sensors: %d",
        dataset_dir.name,
        manifest.get("location", "?"),
        manifest.get("robotType", "?"),
        manifest.get("vehicleId", "?"),
        len(ouster_daqs),
    )

    nav = None
    if apply_nav:
        nav_path = _find_nav_file(dataset_dir)
        if nav_path is not None:
            logger.info("Parsing nav data: %s (%.1f MB)", nav_path.name, nav_path.stat().st_size / 1e6)
            field_names, records = _parse_buf3(nav_path)
            nav = _NavTrajectory(records)
        else:
            logger.info("No navdata2.daq found, importing in lidar frame")

    csv_paths: list[Path] = []
    for i, daq in enumerate(ouster_daqs):
        pcap_path = dataset_dir / daq["pcap"]
        meta_path = _find_metadata(dataset_dir, daq)

        if not pcap_path.exists():
            logger.warning("Skipping %s (not found)", pcap_path.name)
            continue
        if meta_path is None:
            logger.warning("Skipping %s (metadata not found)", daq["info"])
            continue

        parts = pcap_path.stem.split(".")
        sensor_name = parts[2] if len(parts) > 2 else f"ouster_{i}"

        sensor_offset = None
        config_file = daq.get("config")
        if config_file:
            config_path = dataset_dir / config_file
            if config_path.exists():
                sensor_offset = _parse_ouster_params(config_path)

        out_csv = output_dir / f"{sensor_name}.csv"
        logger.info("[%d/%d] Converting %s", i + 1, len(ouster_daqs), sensor_name)
        _convert_sensor(
            pcap_path,
            meta_path,
            out_csv,
            nav,
            manifest.get("playbackStartGmt", 0.0),
            sensor_offset,
            max_scans,
        )
        csv_paths.append(out_csv)

    return csv_paths


# -- buf3 binary parser -------------------------------------------------------


def _parse_buf3(path: Path) -> tuple[list[str], list[dict[str, float]]]:
    data = path.read_bytes()
    assert data[:4] == b"buf3", f"Not a buf3 file: {path}"

    offset = 9  # magic(4) + flags(4) + null(1)

    date_end = offset
    while date_end < len(data) - 4:
        name_len_candidate = struct.unpack_from("<I", data, date_end)[0]
        if 1 <= name_len_candidate <= 255:
            name_start = date_end + 4
            name_bytes = data[name_start : name_start + name_len_candidate]
            if all(32 <= b < 127 for b in name_bytes):
                break
        date_end += 1

    name_len = struct.unpack_from("<I", data, date_end)[0]
    offset = date_end + 4 + name_len

    flist_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    fields_raw = data[offset : offset + flist_len].decode("ascii", errors="replace")
    field_names = [f for f in fields_raw.strip().split(" ") if f]
    offset += flist_len

    records = []
    while offset + 12 < len(data):
        ts = struct.unpack_from("<d", data, offset)[0]
        offset += 8
        count = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        n = count - 1
        if n <= 0 or offset + n * 2 + n * 8 > len(data):
            break
        indices = struct.unpack_from(f"<{n}H", data, offset)
        offset += n * 2
        values = struct.unpack_from(f"<{n}d", data, offset)
        offset += n * 8

        rec = {"_timestamp": ts}
        for idx, val in zip(indices, values):
            fi = idx - 1
            if 0 <= fi < len(field_names):
                rec[field_names[fi]] = val
        records.append(rec)

    return field_names, records


# -- Nav trajectory ------------------------------------------------------------


class _NavTrajectory:
    def __init__(self, records: list[dict[str, float]]) -> None:
        poses = []
        for rec in records:
            ts = rec.get("_timestamp", 0.0)
            if ts < 1e9:
                continue
            tx = rec.get("navdata2tranrelx", 0.0)
            ty = rec.get("navdata2tranrely", 0.0)
            tz = rec.get("navdata2tranrelz", 0.0)
            roll = rec.get("navdata2rpyrelr", 0.0)
            pitch = rec.get("navdata2rpyrelp", 0.0)
            yaw = rec.get("navdata2rpyrely", 0.0)
            poses.append((ts, tx, ty, tz, roll, pitch, yaw))

        poses.sort(key=lambda p: p[0])
        self.timestamps = np.array([p[0] for p in poses])
        self.translations = np.array([(p[1], p[2], p[3]) for p in poses])
        self.rpys = np.array([(p[4], p[5], p[6]) for p in poses])
        logger.info("Nav: %d poses, %.3f – %.3f s", len(poses), self.timestamps[0], self.timestamps[-1])

    def interpolate(self, t: float) -> tuple[np.ndarray, np.ndarray]:
        if t <= self.timestamps[0]:
            return self.translations[0], self.rpys[0]
        if t >= self.timestamps[-1]:
            return self.translations[-1], self.rpys[-1]
        idx = int(np.searchsorted(self.timestamps, t, side="right")) - 1
        i = max(0, min(idx, len(self.timestamps) - 2))
        alpha = (t - self.timestamps[i]) / (self.timestamps[i + 1] - self.timestamps[i])
        trans = self.translations[i] + alpha * (self.translations[i + 1] - self.translations[i])
        rpy = self.rpys[i] + alpha * (self.rpys[i + 1] - self.rpys[i])
        return trans, rpy


# -- Coordinate transforms ----------------------------------------------------


def _rotation_matrix(roll: float, pitch: float, yaw: float) -> np.ndarray:
    cr, sr = np.cos(roll), np.sin(roll)
    cp, sp = np.cos(pitch), np.sin(pitch)
    cy, sy = np.cos(yaw), np.sin(yaw)
    return np.array(
        [
            [cy * cp, cy * sp * sr - sy * cr, cy * sp * cr + sy * sr],
            [sy * cp, sy * sp * sr + cy * cr, sy * sp * cr - cy * sr],
            [-sp, cp * sr, cp * cr],
        ]
    )


_FRD_FLU = np.diag([1.0, -1.0, -1.0])


def _rotation_frd_to_flu(roll: float, pitch: float, yaw: float) -> np.ndarray:
    result: np.ndarray = _FRD_FLU @ _rotation_matrix(roll, pitch, yaw) @ _FRD_FLU
    return result


def _translation_frd_to_flu(t_frd: np.ndarray) -> np.ndarray:
    result: np.ndarray = _FRD_FLU @ t_frd
    return result


# -- Sensor offset parsing ----------------------------------------------------


def _parse_ouster_params(path: Path) -> tuple[np.ndarray, np.ndarray] | None:
    vals: dict[str, float] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        try:
            vals[key.strip()] = float(value.strip())
        except ValueError:
            pass

    if "sensorX" not in vals:
        return None

    trans = np.array([vals.get("sensorX", 0.0), vals.get("sensorY", 0.0), vals.get("sensorZ", 0.0)])
    rpy = np.array([vals.get("sensorRoll", 0.0), vals.get("sensorPitch", 0.0), vals.get("sensorYaw", 0.0)])
    return trans, rpy


# -- Metadata discovery --------------------------------------------------------


def _find_metadata(dataset_dir: Path, daq: dict[str, Any]) -> Path | None:
    info_path = dataset_dir / str(daq["info"])
    studio_path = Path(str(info_path).replace("ousterinfo.", "ousterstudio."))
    if studio_path.exists():
        return studio_path
    if info_path.exists():
        return info_path
    return None


def _find_nav_file(dataset_dir: Path) -> Path | None:
    for folder in sorted(dataset_dir.iterdir()):
        if folder.is_dir() and "nav" in folder.name:
            candidate = folder / "navdata2.daq"
            if candidate.exists():
                return candidate
    root_nav = dataset_dir / "navdata2.daq"
    if root_nav.exists():
        return root_nav
    return None


def _build_structured_metadata(meta_path: Path) -> str:
    import json as _json

    lines = [line.strip() for line in meta_path.read_text().splitlines() if line.strip()]
    if len(lines) >= 6:
        return _json.dumps(
            {
                "lidar_data_format": _json.loads(lines[0]),
                "beam_intrinsics": _json.loads(lines[1]),
                "sensor_info": _json.loads(lines[2]),
                "imu_intrinsics": _json.loads(lines[3]),
                "lidar_intrinsics": _json.loads(lines[4]),
                "config_params": _json.loads(lines[5]),
                "calibration_status": {},
            }
        )
    return meta_path.read_text()


# -- PCAP decoding + CSV writing -----------------------------------------------


def _apply_transforms(
    pts: np.ndarray,
    sensor_offset: tuple[np.ndarray, np.ndarray] | None,
    nav: _NavTrajectory | None,
    frame_gmt: float,
) -> np.ndarray:
    if sensor_offset is not None:
        s_trans_frd, s_rpy = sensor_offset
        r_s = _rotation_frd_to_flu(s_rpy[0], s_rpy[1], s_rpy[2])
        t_s = _translation_frd_to_flu(s_trans_frd)
        pts = (r_s @ pts.T).T + t_s
    if nav is not None:
        trans_frd, rpy = nav.interpolate(frame_gmt)
        r_nav = _rotation_frd_to_flu(rpy[0], rpy[1], rpy[2])
        t_nav = _translation_frd_to_flu(trans_frd)
        pts = (r_nav @ pts.T).T + t_nav
    return pts


def _write_scan_points(
    writer: Any,
    pts: np.ndarray,
    refl: np.ndarray,
    signal: np.ndarray,
    near_ir: np.ndarray,
    time_rel: float,
) -> None:
    for j in range(len(pts)):
        writer.writerow(
            [
                f"{pts[j, 0]:.4f}",
                f"{pts[j, 1]:.4f}",
                f"{pts[j, 2]:.4f}",
                f"{time_rel:.6f}",
                f"{refl[j]:.0f}",
                f"{signal[j]:.0f}",
                f"{near_ir[j]:.0f}",
            ]
        )


def _convert_sensor(
    pcap_path: Path,
    meta_path: Path,
    out_csv: Path,
    nav: _NavTrajectory | None,
    playback_start_gmt: float,
    sensor_offset: tuple[np.ndarray, np.ndarray] | None,
    max_scans: int | None,
) -> int:
    try:
        from ouster.sdk.core import ChanField, XYZLut
        from ouster.sdk.pcap import PcapScanSource
    except ImportError:
        raise ImportError("ouster-sdk is required for PCAP conversion: pip install nominal[ouster]")

    logger.info("Loading metadata: %s", meta_path.name)
    meta_json = _build_structured_metadata(meta_path)

    meta_sidecar = pcap_path.with_suffix(".json")
    meta_sidecar.write_text(meta_json)

    logger.info("Opening PCAP: %s (%.1f MB)", pcap_path.name, pcap_path.stat().st_size / 1e6)
    try:
        source = PcapScanSource(str(pcap_path))
        xyzlut = XYZLut(source.sensor_info[0])

        total_points = 0
        scan_count = 0
        ptp_to_gmt_offset: float | None = None

        with open(out_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["x", "y", "z", "time", "reflectivity", "signal", "near_ir"])

            for scan_set in source:
                if max_scans is not None and scan_count >= max_scans:
                    break

                scan = scan_set[0]
                xyz = xyzlut(scan)
                refl = scan.field(ChanField.REFLECTIVITY).astype(np.float64).reshape(-1)
                sig = scan.field(ChanField.SIGNAL).astype(np.float64).reshape(-1)
                nir = scan.field(ChanField.NEAR_IR).astype(np.float64).reshape(-1)

                valid_ts = scan.timestamp[scan.timestamp > 0]
                if len(valid_ts) == 0:
                    scan_count += 1
                    continue
                frame_ts_s = float(np.median(valid_ts)) / 1e9

                if ptp_to_gmt_offset is None:
                    ptp_to_gmt_offset = playback_start_gmt - frame_ts_s
                    logger.debug("PTP→GMT offset: %.3fs", ptp_to_gmt_offset)

                frame_gmt = frame_ts_s + ptp_to_gmt_offset
                time_rel = frame_gmt - playback_start_gmt

                pts = xyz.reshape(-1, 3)
                valid = np.linalg.norm(pts, axis=1) > 0.1
                pts, refl, sig, nir = pts[valid], refl[valid], sig[valid], nir[valid]

                pts = _apply_transforms(pts, sensor_offset, nav, frame_gmt)
                _write_scan_points(writer, pts, refl, sig, nir, time_rel)

                total_points += len(pts)
                scan_count += 1
                if scan_count % 10 == 0:
                    logger.info("%d scans, %d points...", scan_count, total_points)

    finally:
        meta_sidecar.unlink(missing_ok=True)

    logger.info("Done: %d scans, %d points -> %s", scan_count, total_points, out_csv.name)
    return total_points
