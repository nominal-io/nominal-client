"""Thermocouple emf to temperature, by the ITS-90 inverse functions.

The coefficients are NIST's approximate inverse functions from the ITS-90
Thermocouple Database (NIST Standard Reference Database 60, the
``type_<letter>.tab`` files, section "Inverse coefficients"), a work of the
United States Government in the public domain. Each thermocouple type has two
to four emf ranges, each with its own polynomial ``t = sum(d_i * E**i)`` for
``E`` in millivolts and ``t`` in degrees Celsius.

DAQmx names the type in ``NI_Scale[n]_Thermocouple_Thermocouple_Type`` with
its own enumeration, mapped in ``DAQMX_TYPES``.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class _Range:
    emf_min: float  # mV
    emf_max: float
    coefficients: tuple[float, ...]  # constant term first


_INVERSE: dict[str, tuple[_Range, ...]] = {
    "B": (
        # 250 to 700 degC
        _Range(
            0.291,
            2.431,
            (98.423321, 699.715, -847.65304, 1005.2644, -833.45952, 455.08542, -155.23037, 29.88675, -2.474286),
        ),
        # 700 to 1820 degC
        _Range(
            2.431,
            13.82,
            (
                213.15071,
                285.10504,
                -52.742887,
                9.9160804,
                -1.2965303,
                0.1119587,
                -0.0060625199,
                0.00018661696,
                -2.4878585e-06,
            ),
        ),
    ),
    "E": (
        # -200 to 0 degC
        _Range(
            -8.825,
            0.0,
            (
                0.0,
                16.977288,
                -0.4351497,
                -0.15859697,
                -0.092502871,
                -0.026084314,
                -0.0041360199,
                -0.0003403403,
                -1.156489e-05,
            ),
        ),
        # 0 to 1000 degC
        _Range(
            0.0,
            76.373,
            (
                0.0,
                17.057035,
                -0.23301759,
                0.0065435585,
                -7.3562749e-05,
                -1.7896001e-06,
                8.4036165e-08,
                -1.3735879e-09,
                1.0629823e-11,
                -3.2447087e-14,
            ),
        ),
    ),
    "J": (
        # -210 to 0 degC
        _Range(
            -8.095,
            0.0,
            (
                0.0,
                19.528268,
                -1.2286185,
                -1.0752178,
                -0.59086933,
                -0.17256713,
                -0.028131513,
                -0.002396337,
                -8.3823321e-05,
            ),
        ),
        # 0 to 760 degC
        _Range(
            0.0,
            42.919,
            (0.0, 19.78425, -0.2001204, 0.01036969, -0.0002549687, 3.585153e-06, -5.344285e-08, 5.09989e-10),
        ),
        # 760 to 1200 degC
        _Range(42.919, 69.553, (-3113.58187, 300.543684, -9.9477323, 0.17027663, -0.00143033468, 4.73886084e-06)),
    ),
    "K": (
        # -200 to 0 degC
        _Range(
            -5.891,
            0.0,
            (
                0.0,
                25.173462,
                -1.1662878,
                -1.0833638,
                -0.8977354,
                -0.37342377,
                -0.086632643,
                -0.010450598,
                -0.00051920577,
            ),
        ),
        # 0 to 500 degC
        _Range(
            0.0,
            20.644,
            (
                0.0,
                25.08355,
                0.07860106,
                -0.2503131,
                0.0831527,
                -0.01228034,
                0.0009804036,
                -4.41303e-05,
                1.057734e-06,
                -1.052755e-08,
            ),
        ),
        # 500 to 1372 degC
        _Range(20.644, 54.886, (-131.8058, 48.30222, -1.646031, 0.05464731, -0.0009650715, 8.802193e-06, -3.11081e-08)),
    ),
    "N": (
        # -200 to 0 degC
        _Range(
            -3.99,
            0.0,
            (
                0.0,
                38.436847,
                1.1010485,
                5.2229312,
                7.2060525,
                5.8488586,
                2.7754916,
                0.77075166,
                0.11582665,
                0.0073138868,
            ),
        ),
        # 0 to 600 degC
        _Range(0.0, 20.613, (0.0, 38.6896, -1.08267, 0.0470205, -2.12169e-06, -0.000117272, 5.3928e-06, -7.98156e-08)),
        # 600 to 1300 degC
        _Range(20.613, 47.513, (19.72485, 33.00943, -0.3915159, 0.009855391, -0.0001274371, 7.767022e-07)),
    ),
    "R": (
        # -50 to 250 degC
        _Range(
            -0.226,
            1.923,
            (
                0.0,
                188.9138,
                -93.83529,
                130.68619,
                -227.0358,
                351.45659,
                -389.539,
                282.39471,
                -126.07281,
                31.353611,
                -3.3187769,
            ),
        ),
        # 250 to 1200 degC
        _Range(
            1.923,
            13.228,
            (
                13.34584505,
                147.2644573,
                -18.44024844,
                4.031129726,
                -0.624942836,
                0.06468412046,
                -0.004458750426,
                0.0001994710149,
                -5.31340179e-06,
                6.481976217e-08,
            ),
        ),
        # 1064 to 1664.5 degC
        _Range(11.361, 19.739, (-81.99599416, 155.3962042, -8.342197663, 0.4279433549, -0.0119157791, 0.0001492290091)),
        # 1664.5 to 1768.1 degC
        _Range(19.739, 21.103, (34061.77836, -7023.729171, 558.2903813, -19.52394635, 0.2560740231)),
    ),
    "S": (
        # -50 to 250 degC
        _Range(
            -0.235,
            1.874,
            (
                0.0,
                184.94946,
                -80.0504062,
                102.23743,
                -152.248592,
                188.821343,
                -159.085941,
                82.302788,
                -23.4181944,
                2.7978626,
            ),
        ),
        # 250 to 1200 degC
        _Range(
            1.874,
            11.95,
            (
                12.91507177,
                146.6298863,
                -15.34713402,
                3.145945973,
                -0.4163257839,
                0.03187963771,
                -0.0012916375,
                2.183475087e-05,
                -1.447379511e-07,
                8.211272125e-09,
            ),
        ),
        # 1064 to 1664.5 degC
        _Range(10.332, 17.536, (-80.87801117, 162.1573104, -8.536869453, 0.4719686976, -0.01441693666, 0.000208161889)),
        # 1664.5 to 1768.1 degC
        _Range(17.536, 18.693, (53338.75126, -12358.92298, 1092.657613, -42.65693686, 0.624720542)),
    ),
    "T": (
        # -200 to 0 degC
        _Range(
            -5.603, 0.0, (0.0, 25.949192, -0.21316967, 0.79018692, 0.42527777, 0.13304473, 0.020241446, 0.0012668171)
        ),
        # 0 to 400 degC
        _Range(0.0, 20.872, (0.0, 25.928, -0.7602961, 0.04637791, -0.002165394, 6.048144e-05, -7.293422e-07)),
    ),
}

# DAQmx's ThermocoupleType enumeration.
DAQMX_TYPES = {10047: "B", 10055: "E", 10072: "J", 10073: "K", 10077: "N", 10082: "R", 10085: "S", 10086: "T"}


def to_temperature(emf_mv: np.ndarray, letter: str) -> np.ndarray:
    """Degrees Celsius for thermocouple emf in millivolts (cold junction already compensated).

    Each value uses the polynomial of the emf range it falls in; below the
    first range or above the last, the nearest range's polynomial is
    extrapolated rather than yielding NaN.
    """
    ranges = _INVERSE[letter]
    emf = np.asarray(emf_mv, dtype=np.float64)
    out = np.empty_like(emf)
    assigned = np.zeros(emf.shape, dtype=bool)
    for r in ranges:
        mask = ~assigned & (emf >= r.emf_min) & (emf <= r.emf_max)
        if mask.any():
            out[mask] = np.polynomial.polynomial.polyval(emf[mask], r.coefficients)
            assigned |= mask
    low = ~assigned & (emf < ranges[0].emf_min)
    high = ~assigned & ~low
    if low.any():
        out[low] = np.polynomial.polynomial.polyval(emf[low], ranges[0].coefficients)
    if high.any():
        out[high] = np.polynomial.polynomial.polyval(emf[high], ranges[-1].coefficients)
    return out
