"""NI scaling: the ``NI_Scale[n]_*`` properties DAQmx writes beside unscaled data.

When DAQmx logs a task to TDMS it stores the device's raw integers and, on the
channel, a description of how to turn them into engineering units. That
description is a small graph of stages. Stage ``n`` names its type in
``NI_Scale[n]_Scale_Type`` and its input in ``NI_Scale[n]_<Type>_Input_Source``,
where the input is another stage's number or the ``scale id`` of one of the
channel's raw DAQmx scalers (stage numbers and scale ids share one numbering).
The channel's value is the output of stage ``NI_Number_Of_Scales - 1``.

A plain voltage channel is one Linear stage over raw scaler 0. A custom scale
adds a Polynomial or Table stage on top. A thermocouple channel reads two raw
scalers (signal and cold junction), scales each, subtracts, and finishes with a
Thermocouple stage. The stage types below are the ones needed for files seen so
far; anything else raises ``UnsupportedScaling`` so the caller can exclude the
channel rather than the file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import numpy.typing as npt

from nominal.tdms import _thermocouples as thermocouples

_STAGE_TYPE = re.compile(r"^NI_Scale\[(\d+)\]_Scale_Type$")


class UnsupportedScaling(Exception):
    """The channel's scale graph uses a stage this module does not implement."""


@dataclass(frozen=True)
class Stage:
    inputs: tuple[int, ...]
    apply: Callable[..., npt.NDArray[Any]]  # one float64 array per input, in order


@dataclass(frozen=True)
class Scaling:
    stages: dict[int, Stage]
    output: int
    order: tuple[int, ...]  # the stages reachable from the output, inputs before their readers

    @property
    def raw_ids(self) -> frozenset[int]:
        """The raw scaler ids the graph reads."""
        return frozenset(i for stage in self.stages.values() for i in stage.inputs if i not in self.stages)

    def apply(self, raw: dict[int, npt.NDArray[Any]]) -> npt.NDArray[Any]:
        """Evaluate the graph over the raw scaler arrays (keyed by scale id)."""
        values: dict[int, npt.NDArray[Any]] = {}
        for i in self.order:
            stage = self.stages[i]
            values[i] = stage.apply(
                *(values[j] if j in self.stages else raw[j].astype(np.float64, copy=False) for j in stage.inputs)
            )
        return values[self.output]


def _number(properties: dict[str, object], key: str, path: str) -> float:
    try:
        return float(properties[key])  # type: ignore[arg-type]
    except (KeyError, TypeError, ValueError) as e:
        raise UnsupportedScaling(f"{path}: NI scaling property {key} is missing or not a number") from e


def _int(properties: dict[str, object], key: str, path: str) -> int:
    value = properties.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise UnsupportedScaling(f"{path}: NI scaling property {key} is missing or not an integer")
    return value


def _array(properties: dict[str, object], prefix: str, path: str) -> npt.NDArray[Any]:
    size = _int(properties, f"{prefix}_Size", path)
    return np.array([_number(properties, f"{prefix}[{i}]", path) for i in range(size)], dtype=np.float64)


def _linear(properties: dict[str, object], n: int, path: str) -> Stage:
    p = f"NI_Scale[{n}]_Linear"
    slope, intercept = _number(properties, f"{p}_Slope", path), _number(properties, f"{p}_Y_Intercept", path)
    return Stage((_int(properties, f"{p}_Input_Source", path),), lambda x: x * slope + intercept)


def _polynomial(properties: dict[str, object], n: int, path: str) -> Stage:
    p = f"NI_Scale[{n}]_Polynomial"
    coefficients = _array(properties, f"{p}_Coefficients", path)
    if coefficients.size == 0:
        raise UnsupportedScaling(f"{path}: polynomial scale {n} has no coefficients")
    return Stage(
        (_int(properties, f"{p}_Input_Source", path),), lambda x: np.polynomial.polynomial.polyval(x, coefficients)
    )


def _table(properties: dict[str, object], n: int, path: str) -> Stage:
    """Piecewise-linear interpolation, clamped at the table's ends.

    The names are the reverse of what they suggest: the stage's input runs
    along ``Scaled_Values`` and its output along ``Pre_Scaled_Values``. That is
    what DAQmx itself computes for the recordings under tests/fixtures/ni
    (``daqmx_scale_table.tdms``: about 50 percent for 0.05 V with a table
    mapping -10..10 V onto 0..100); other readers have been seen to get it the
    other way round.
    """
    p = f"NI_Scale[{n}]_Table"
    inputs = _array(properties, f"{p}_Scaled_Values", path)
    outputs = _array(properties, f"{p}_Pre_Scaled_Values", path)
    if inputs.size != outputs.size or inputs.size < 2:
        raise UnsupportedScaling(f"{path}: table scale {n} needs at least two matching pre-scaled and scaled values")
    order = np.argsort(inputs)
    inputs, outputs = inputs[order], outputs[order]
    if np.any(np.diff(inputs) <= 0):
        raise UnsupportedScaling(f"{path}: table scale {n} has repeated input values")
    return Stage((_int(properties, f"{p}_Input_Source", path),), lambda x: np.interp(x, inputs, outputs))


def _binary(
    op: Callable[[npt.NDArray[Any], npt.NDArray[Any]], npt.NDArray[Any]], kind: str
) -> Callable[[dict[str, object], int, str], Stage]:
    def build(properties: dict[str, object], n: int, path: str) -> Stage:
        p = f"NI_Scale[{n}]_{kind}"
        left = _int(properties, f"{p}_Left_Operand_Input_Source", path)
        right = _int(properties, f"{p}_Right_Operand_Input_Source", path)
        return Stage((left, right), op)

    return build


def _thermocouple(properties: dict[str, object], n: int, path: str) -> Stage:
    """Compensated thermocouple emf in microvolts to degrees Celsius."""
    p = f"NI_Scale[{n}]_Thermocouple"
    code = _int(properties, f"{p}_Thermocouple_Type", path)
    letter = thermocouples.DAQMX_TYPES.get(code)
    if letter is None:
        raise UnsupportedScaling(f"{path}: thermocouple type {code} is not supported")
    if _int(properties, f"{p}_Scaling_Direction", path) != 0:
        raise UnsupportedScaling(f"{path}: thermocouple scaling from temperature to voltage is not supported")
    return Stage(
        (_int(properties, f"{p}_Input_Source", path),), lambda x: thermocouples.to_temperature(x / 1000.0, letter)
    )


def _rtd(properties: dict[str, object], n: int, path: str) -> Stage:
    """Voltage across an RTD to degrees Celsius by the Callendar-Van Dusen equation.

    Resistance is voltage over the excitation current, less the lead wires for
    a 2-wire connection. Above 0 degC the quadratic ``R = R0 (1 + A t + B t^2)``
    inverts in closed form; below, the quartic with the C term is solved by
    Newton's method from that starting point.

    DAQmx itself never writes a 2-wire RTD scale -- its RTD modules refuse the
    configuration (NI KB kA00Z0000019MQSSA2) -- so that branch has no recorded
    ground truth; it follows the ``R - 2 R_lead`` convention the
    daqmx_thermistor_iex_2wire recording pins for the thermistor scale.
    """
    p = f"NI_Scale[{n}]_RTD"
    current = _number(properties, f"{p}_Current_Excitation", path)
    r0 = _number(properties, f"{p}_R0_Nominal_Resistance", path)
    a, b, c = (_number(properties, f"{p}_{k}", path) for k in ("A", "B", "C"))
    lead = _number(properties, f"{p}_Lead_Wire_Resistance", path)
    wires = _int(properties, f"{p}_Resistance_Configuration", path)
    if current == 0 or r0 == 0 or b == 0:
        raise UnsupportedScaling(f"{path}: RTD scale {n} has a zero excitation current, R0 or B coefficient")

    def apply(voltage: npt.NDArray[Any]) -> npt.NDArray[Any]:
        resistance = voltage / current - (2.0 * lead if wires == 2 else 0.0)
        ratio = resistance / r0
        with np.errstate(invalid="ignore"):
            t = (-a + np.sqrt(a * a - 4.0 * b * (1.0 - ratio))) / (2.0 * b)
        below = ratio < 1.0
        if c != 0 and below.any():
            tb = t[below]
            rb = ratio[below]
            for _ in range(8):
                f = 1.0 + a * tb + b * tb**2 + c * (tb - 100.0) * tb**3 - rb
                df = a + 2.0 * b * tb + c * (4.0 * tb**3 - 300.0 * tb**2)
                tb = tb - f / df
            t[below] = tb
        return t

    return Stage((_int(properties, f"{p}_Input_Source", path),), apply)


_EXCITATION_VOLTAGE, _EXCITATION_CURRENT = 10322, 10134  # DAQmx ExcitationSource-style codes


def _thermistor(properties: dict[str, object], n: int, path: str) -> Stage:
    """Voltage across a thermistor to kelvin by the Steinhart-Hart equation.

    With voltage excitation the thermistor is measured in a divider with the
    reference resistor R1, so ``R = R1 * V / (Vex - V)``; with current
    excitation ``R = V / Iex``. Then ``1/T = A + B ln R + C (ln R)^3``.
    """
    p = f"NI_Scale[{n}]_Thermistor"
    excitation_type = _int(properties, f"{p}_Excitation_Type", path)
    excitation = _number(properties, f"{p}_Excitation_Value", path)
    r1 = _number(properties, f"{p}_R1_Reference_Resistance", path)
    lead = _number(properties, f"{p}_Lead_Wire_Resistance", path)
    wires = _int(properties, f"{p}_Resistance_Configuration", path)
    a, b, c = (_number(properties, f"{p}_{k}", path) for k in ("A", "B", "C"))
    offset = _number(properties, f"{p}_Temperature_Offset", path)
    if excitation_type not in (_EXCITATION_VOLTAGE, _EXCITATION_CURRENT):
        raise UnsupportedScaling(f"{path}: thermistor excitation type {excitation_type} is not supported")
    if excitation == 0:
        raise UnsupportedScaling(f"{path}: thermistor scale {n} has zero excitation")

    def apply(voltage: npt.NDArray[Any]) -> npt.NDArray[Any]:
        with np.errstate(divide="ignore", invalid="ignore"):
            if excitation_type == _EXCITATION_VOLTAGE:
                resistance = r1 * voltage / (excitation - voltage)
            else:
                resistance = voltage / excitation
            resistance = resistance - (2.0 * lead if wires == 2 else 0.0)
            ln_r = np.log(resistance)
            return 1.0 / (a + b * ln_r + c * ln_r**3) + offset

    return Stage((_int(properties, f"{p}_Input_Source", path),), apply)


# DAQmx StrainGageBridgeType codes -> strain from the bridge ratio Vr, gage factor
# GF and Poisson ratio v, per NI's strain gauge measurement documentation.
_STRAIN_BRIDGES: dict[int, Callable[[npt.NDArray[Any], float, float], npt.NDArray[Any]]] = {
    10183: lambda vr, gf, v: -vr / gf,  # full bridge I
    10184: lambda vr, gf, v: -2.0 * vr / (gf * (v + 1.0)),  # full bridge II
    10185: lambda vr, gf, v: -2.0 * vr / (gf * ((v + 1.0) - vr * (v - 1.0))),  # full bridge III
    10188: lambda vr, gf, v: -4.0 * vr / (gf * ((1.0 + v) - 2.0 * vr * (v - 1.0))),  # half bridge I
    10189: lambda vr, gf, v: -2.0 * vr / gf,  # half bridge II
    10271: lambda vr, gf, v: -4.0 * vr / (gf * (1.0 + 2.0 * vr)),  # quarter bridge I
    10272: lambda vr, gf, v: -4.0 * vr / (gf * (1.0 + 2.0 * vr)),  # quarter bridge II
}
# Configurations whose gage sits in one arm, where lead resistance desensitises it.
_STRAIN_LEAD_CORRECTED = {10188, 10189, 10271, 10272}


def _strain(properties: dict[str, object], n: int, path: str) -> Stage:
    """Bridge output voltage to strain for a Wheatstone-bridge gage configuration."""
    p = f"NI_Scale[{n}]_Strain"
    configuration = _int(properties, f"{p}_Configuration", path)
    formula = _STRAIN_BRIDGES.get(configuration)
    if formula is None:
        raise UnsupportedScaling(f"{path}: strain bridge configuration {configuration} is not supported")
    poisson = _number(properties, f"{p}_Poisson_Ratio", path)
    gage_resistance = _number(properties, f"{p}_Gage_Resistance", path)
    lead = _number(properties, f"{p}_Lead_Wire_Resistance", path)
    initial = _number(properties, f"{p}_Initial_Bridge_Voltage", path)
    gage_factor = _number(properties, f"{p}_Gage_Factor", path)
    gain = _number(properties, f"{p}_Bridge_Shunt_Calibration_Gain_Adjustment", path)
    excitation = _number(properties, f"{p}_Voltage_Excitation", path)
    if excitation == 0 or gage_factor == 0 or gage_resistance == 0:
        raise UnsupportedScaling(f"{path}: strain scale {n} has a zero excitation, gage factor or gage resistance")
    lead_factor = 1.0 + lead / gage_resistance if configuration in _STRAIN_LEAD_CORRECTED else 1.0

    def apply(voltage: npt.NDArray[Any]) -> npt.NDArray[Any]:
        vr = (voltage - initial) / excitation
        return formula(vr, gage_factor, poisson) * lead_factor * gain

    return Stage((_int(properties, f"{p}_Input_Source", path),), apply)


_BUILDERS: dict[str, Callable[[dict[str, object], int, str], Stage]] = {
    "RTD": _rtd,
    "Thermistor": _thermistor,
    "Strain": _strain,
    "Linear": _linear,
    "Polynomial": _polynomial,
    "Table": _table,
    # Right minus left: established against NI's own TDMS viewer on a DAQmx
    # thermocouple recording (tests/fixtures/ni/daqmx_thermocouple.tdms), where
    # left is the signal stage and right the cold-junction stage.
    "Subtract": _binary(lambda left, right: right - left, "Subtract"),
    "Add": _binary(lambda a, b: a + b, "Add"),
    "Thermocouple": _thermocouple,
}


def parse_scaling(properties: dict[str, object], path: str) -> Scaling | None:
    """The channel's scale graph, or None when its data is already in final units.

    Raises UnsupportedScaling for a stage type or shape this module cannot evaluate.
    """
    if properties.get("NI_Scaling_Status") == "scaled":
        return None
    numbers = sorted(int(m.group(1)) for name in properties if (m := _STAGE_TYPE.match(name)))
    if not numbers:
        return None
    stages: dict[int, Stage] = {}
    for n in numbers:
        kind = properties[f"NI_Scale[{n}]_Scale_Type"]
        builder = _BUILDERS.get(str(kind))
        if builder is None:
            raise UnsupportedScaling(f"{path}: NI scale type {kind!r} is not supported")
        stages[n] = builder(properties, n, path)
    count = properties.get("NI_Number_Of_Scales")
    output = (
        count - 1 if isinstance(count, int) and not isinstance(count, bool) and count - 1 in stages else numbers[-1]
    )
    # Order the stages reachable from the output, inputs first, rejecting
    # cycles. Iteratively: recursion would overflow on a graph thousands of
    # stages deep, and RecursionError would escape as neither a TdmsError nor
    # an exclusion.
    order: list[int] = []
    visiting: set[int] = set()
    done: set[int] = set()
    stack: list[tuple[int, bool]] = [(output, False)]
    while stack:
        i, inputs_ordered = stack.pop()
        if i not in stages or i in done:
            continue
        if inputs_ordered:
            visiting.discard(i)
            done.add(i)
            order.append(i)
            continue
        if i in visiting:
            raise UnsupportedScaling(f"{path}: NI scaling stages form a cycle")
        visiting.add(i)
        stack.append((i, True))
        stack.extend((j, False) for j in stages[i].inputs)
    return Scaling(stages, output, tuple(order))
