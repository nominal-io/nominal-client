"""Unit tests for adaptive video normalization. All logic is tested with pure inputs; no real ffmpeg runs."""

from __future__ import annotations

import pathlib
from fractions import Fraction

import pytest

from nominal.experimental.video_processing.video_conversion import (
    NormalizationError,
    TimingStrategy,
    _build_output_kwargs,
    _classify_tier1,
    _clean_encode_kwargs,
    _deltas_match_grid,
    _has_timestamp_discontinuity,
    _parse_packet_pts,
    _parse_rate,
    _parse_timing_probe,
    _select_strategy,
    _timing_output_kwargs,
    _verify_invariants,
    normalize_video,
)


class TestParseRate:
    def test_fractional_rate(self) -> None:
        assert _parse_rate("30000/1001") == Fraction(30000, 1001)

    def test_integer_rate(self) -> None:
        assert _parse_rate("25") == Fraction(25, 1)

    def test_none(self) -> None:
        assert _parse_rate(None) is None

    def test_empty(self) -> None:
        assert _parse_rate("") is None

    @pytest.mark.parametrize("bad", ["0/0", "0/30", "30/0", "abc", "-5/1", "/"])
    def test_invalid_or_nonpositive_is_none(self, bad: str) -> None:
        assert _parse_rate(bad) is None


class TestClassifyTier1:
    def test_clean_cfr_when_avg_equals_r(self) -> None:
        r = Fraction(30000, 1001)
        assert _classify_tier1(r, r) is TimingStrategy.CFR_HEAL

    def test_clean_cfr_within_tolerance(self) -> None:
        # 2997/100 (29.97) vs 30000/1001 (29.97003) -> well within tolerance
        assert _classify_tier1(Fraction(30000, 1001), Fraction(2997, 100)) is TimingStrategy.CFR_HEAL

    def test_indeterminate_rate_is_passthrough(self) -> None:
        assert _classify_tier1(None, Fraction(30, 1)) is TimingStrategy.PASSTHROUGH

    def test_missing_avg_is_ambiguous(self) -> None:
        assert _classify_tier1(Fraction(30, 1), None) is None

    def test_large_gap_below_r_is_ambiguous(self) -> None:
        assert _classify_tier1(Fraction(30, 1), Fraction(25, 1)) is None

    def test_avg_greater_than_r_is_ambiguous(self) -> None:
        # avg > r signals misdetected r; must NOT shortcut to CFR (would drop frames)
        assert _classify_tier1(Fraction(25, 1), Fraction(50, 1)) is None


_R30 = Fraction(30, 1)  # base interval 1/30 s


class TestDiscontinuity:
    def test_monotonic_is_clean(self) -> None:
        pts = [i / 30 for i in range(20)]
        assert _has_timestamp_discontinuity(pts, _R30) is False

    def test_small_reordering_ignored(self) -> None:
        # B-frame style local reorder (a few frames) is not a discontinuity
        pts = [0.0, 2 / 30, 1 / 30, 3 / 30, 5 / 30, 4 / 30]
        assert _has_timestamp_discontinuity(pts, _R30) is False

    def test_large_backward_jump_is_discontinuity(self) -> None:
        pts = [0.0, 1 / 30, 2 / 30, 95443.0, 95443.0 + 1 / 30, 0.5]
        assert _has_timestamp_discontinuity(pts, _R30) is True


class TestGridMatch:
    def test_perfect_grid(self) -> None:
        pts = [i / 30 for i in range(50)]
        assert _deltas_match_grid(pts, _R30) is True

    def test_grid_with_holes(self) -> None:
        # drop frames 3 and 7 -> gaps of 2*base, still on-grid
        idx = [0, 1, 2, 4, 5, 6, 8, 9, 10]
        pts = [i / 30 for i in idx]
        assert _deltas_match_grid(pts, _R30) is True

    def test_variable_frame_rate_is_not_grid(self) -> None:
        pts = [0.0, 0.02, 0.05, 0.07, 0.13, 0.18, 0.20, 0.27]
        assert _deltas_match_grid(pts, _R30) is False

    def test_unsorted_grid_still_matches(self) -> None:
        pts = [3 / 30, 0.0, 2 / 30, 1 / 30, 4 / 30]
        assert _deltas_match_grid(pts, _R30) is True


class TestTimingOutputKwargs:
    def test_passthrough(self) -> None:
        assert _timing_output_kwargs(TimingStrategy.PASSTHROUGH, None, None, None) == {"fps_mode": "passthrough"}

    def test_cfr_heal_with_clamp(self) -> None:
        kwargs = _timing_output_kwargs(TimingStrategy.CFR_HEAL, Fraction(30000, 1001), 1110.346, 1109.0)
        assert kwargs["r"] == "30000/1001"
        assert kwargs["fps_mode"] == "cfr"
        assert kwargs["video_track_timescale"] == "30000"
        assert kwargs["t"] == "1110.346000"

    def test_cfr_heal_numerator_timescale_for_5994(self) -> None:
        kwargs = _timing_output_kwargs(TimingStrategy.CFR_HEAL, Fraction(60000, 1001), 10.0, 9.0)
        assert kwargs["video_track_timescale"] == "60000"

    def test_cfr_heal_skips_clamp_when_pts_exceeds_duration(self) -> None:
        # duration under-reports (a sampled frame is later than it) -> do not clamp, would cut frames
        kwargs = _timing_output_kwargs(TimingStrategy.CFR_HEAL, Fraction(30, 1), 10.0, 12.0)
        assert "t" not in kwargs

    def test_cfr_heal_skips_clamp_when_duration_unknown(self) -> None:
        kwargs = _timing_output_kwargs(TimingStrategy.CFR_HEAL, Fraction(30, 1), None, None)
        assert "t" not in kwargs

    def test_cfr_heal_requires_rate(self) -> None:
        with pytest.raises(NormalizationError):
            _timing_output_kwargs(TimingStrategy.CFR_HEAL, None, 10.0, None)


class TestCleanEncodeKwargs:
    def test_h264_defaults(self) -> None:
        assert _clean_encode_kwargs("h264") == {
            "vcodec": "h264",
            "acodec": "aac",
            "pix_fmt": "yuv420p",
            "bf": "0",
        }

    def test_passes_codec_through(self) -> None:
        assert _clean_encode_kwargs("hevc_nvenc")["vcodec"] == "hevc_nvenc"

    def test_disables_b_frames(self) -> None:
        assert _clean_encode_kwargs("hevc")["bf"] == "0"


_P = pathlib.Path("dummy.mp4")


class TestVerifyInvariants:
    def test_cfr_heal_ok(self) -> None:
        # 1110.346s * 29.97003 fps = ~33276 frames
        _verify_invariants(TimingStrategy.CFR_HEAL, Fraction(30000, 1001), 1110.346, None, 33276, 1110.349, _P)

    def test_cfr_heal_frame_shortfall_raises(self) -> None:
        with pytest.raises(NormalizationError, match="expected"):
            _verify_invariants(TimingStrategy.CFR_HEAL, Fraction(30000, 1001), 1110.346, None, 33000, 1110.0, _P)

    def test_cfr_heal_duration_drift_raises(self) -> None:
        with pytest.raises(NormalizationError, match="duration"):
            _verify_invariants(TimingStrategy.CFR_HEAL, Fraction(30000, 1001), 1110.346, None, 33276, 1108.0, _P)

    def test_passthrough_ok(self) -> None:
        _verify_invariants(TimingStrategy.PASSTHROUGH, None, 10.0, 300, 300, 10.0, _P)

    def test_passthrough_dropped_frames_raises(self) -> None:
        with pytest.raises(NormalizationError, match="dropped"):
            _verify_invariants(TimingStrategy.PASSTHROUGH, None, 10.0, 300, 298, 10.0, _P)

    def test_zero_frames_raises(self) -> None:
        with pytest.raises(NormalizationError, match="no frames"):
            _verify_invariants(TimingStrategy.PASSTHROUGH, None, 10.0, 0, 0, 0.0, _P)


class TestParseTimingProbe:
    def test_parses_rates_and_duration(self) -> None:
        probe = {
            "streams": [{"r_frame_rate": "30000/1001", "avg_frame_rate": "2997/100"}],
            "format": {"duration": "3.003"},
        }
        r, avg, dur = _parse_timing_probe(probe, _P)
        assert r == Fraction(30000, 1001)
        assert avg == Fraction(2997, 100)
        assert dur == pytest.approx(3.003)

    def test_no_video_stream_raises(self) -> None:
        with pytest.raises(NormalizationError, match="no video stream"):
            _parse_timing_probe({"streams": [], "format": {}}, _P)

    def test_missing_duration_is_none(self) -> None:
        probe = {"streams": [{"r_frame_rate": "25/1", "avg_frame_rate": "25/1"}], "format": {}}
        _, _, dur = _parse_timing_probe(probe, _P)
        assert dur is None

    def test_indeterminate_rates_are_none(self) -> None:
        probe = {"streams": [{"r_frame_rate": "0/0", "avg_frame_rate": "0/0"}], "format": {"duration": "5.0"}}
        r, avg, dur = _parse_timing_probe(probe, _P)
        assert r is None
        assert avg is None
        assert dur == pytest.approx(5.0)


class TestParsePacketPts:
    def test_returns_pts_in_file_order_skipping_unparseable(self) -> None:
        probe = {
            "packets": [
                {"pts_time": "0.0"},
                {"pts_time": "0.033"},
                {"pts_time": "bad"},
                {"pts_time": "0.066"},
            ]
        }
        assert _parse_packet_pts(probe) == [0.0, 0.033, 0.066]

    def test_no_packets_is_empty(self) -> None:
        assert _parse_packet_pts({}) == []


_VPATH = pathlib.Path("v.mp4")


def _never() -> list[float]:
    raise AssertionError("sample_pts should not be called for this case")


class TestSelectStrategy:
    def test_forced_passthrough(self) -> None:
        assert _select_strategy("passthrough", Fraction(30, 1), Fraction(30, 1), _never, _VPATH) == (
            TimingStrategy.PASSTHROUGH,
            None,
        )

    def test_forced_cfr(self) -> None:
        assert _select_strategy("cfr", Fraction(30, 1), None, _never, _VPATH) == (
            TimingStrategy.CFR_HEAL,
            None,
        )

    def test_forced_cfr_without_rate_raises(self) -> None:
        with pytest.raises(NormalizationError, match="indeterminate"):
            _select_strategy("cfr", None, None, _never, _VPATH)

    def test_auto_clean_cfr_skips_sampling(self) -> None:
        r = Fraction(30000, 1001)
        assert _select_strategy("auto", r, r, _never, _VPATH) == (TimingStrategy.CFR_HEAL, None)

    def test_auto_indeterminate_passthrough_skips_sampling(self) -> None:
        assert _select_strategy("auto", None, Fraction(30, 1), _never, _VPATH) == (
            TimingStrategy.PASSTHROUGH,
            None,
        )

    def test_auto_empty_pts_is_passthrough(self) -> None:
        assert _select_strategy("auto", Fraction(30, 1), Fraction(20, 1), lambda: [], _VPATH) == (
            TimingStrategy.PASSTHROUGH,
            None,
        )

    def test_auto_grid_with_holes_is_cfr_heal(self) -> None:
        pts = [i / 30 for i in [0, 1, 2, 4, 5, 6, 8, 9, 10]]
        strategy, max_pts = _select_strategy("auto", Fraction(30, 1), Fraction(25, 1), lambda: pts, _VPATH)
        assert strategy is TimingStrategy.CFR_HEAL
        assert max_pts == max(pts)

    def test_auto_vfr_is_passthrough(self) -> None:
        pts = [0.0, 0.02, 0.05, 0.07, 0.13, 0.18, 0.20, 0.27]
        strategy, max_pts = _select_strategy("auto", Fraction(30, 1), Fraction(20, 1), lambda: pts, _VPATH)
        assert strategy is TimingStrategy.PASSTHROUGH
        assert max_pts == max(pts)

    def test_auto_discontinuity_raises(self) -> None:
        pts = [0.0, 1 / 30, 2 / 30, 95443.0, 95443.0 + 1 / 30, 0.5]
        with pytest.raises(NormalizationError, match="discontinuity"):
            _select_strategy("auto", Fraction(30, 1), Fraction(20, 1), lambda: pts, _VPATH)


class TestNormalizeVideoValidation:
    """Input-validation errors that need no ffmpeg (the checks run before any probe)."""

    def test_bad_output_suffix_raises(self, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "in.mp4"
        src.write_bytes(b"not really a video")
        with pytest.raises(NormalizationError, match="mkv or .mp4"):
            normalize_video(src, tmp_path / "out.avi")

    def test_missing_input_raises(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(NormalizationError, match="does not exist"):
            normalize_video(tmp_path / "nope.mp4", tmp_path / "out.mp4")

    def test_force_false_existing_output_raises(self, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "in.mp4"
        src.write_bytes(b"x")
        out = tmp_path / "out.mp4"
        out.write_bytes(b"existing")
        with pytest.raises(NormalizationError, match="already exists"):
            normalize_video(src, out, force=False)


class TestBuildOutputKwargs:
    def test_cfr_heal_includes_clean_encode_and_timing(self) -> None:
        kwargs = _build_output_kwargs(TimingStrategy.CFR_HEAL, Fraction(30000, 1001), 10.0, None, "h264", 2, None)
        # clean-encode layer
        assert kwargs["vcodec"] == "h264"
        assert kwargs["pix_fmt"] == "yuv420p"
        assert kwargs["bf"] == "0"
        # timing layer
        assert kwargs["fps_mode"] == "cfr"
        assert kwargs["r"] == "30000/1001"
        assert kwargs["video_track_timescale"] == "30000"
        # keyframes
        assert kwargs["force_key_frames"] == "expr:gte(t,n_forced*2)"
        assert "vf" not in kwargs

    def test_passthrough_uses_passthrough_fps_mode(self) -> None:
        kwargs = _build_output_kwargs(TimingStrategy.PASSTHROUGH, None, 10.0, None, "hevc", 2, None)
        assert kwargs["fps_mode"] == "passthrough"
        assert kwargs["vcodec"] == "hevc"
        assert kwargs["bf"] == "0"
        assert "r" not in kwargs

    def test_key_frame_interval_none_uses_source(self) -> None:
        kwargs = _build_output_kwargs(TimingStrategy.PASSTHROUGH, None, None, None, "h264", None, None)
        assert kwargs["force_key_frames"] == "source"
