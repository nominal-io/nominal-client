# Recorded NI files

TDMS files written by NI software — DAQmx logging and LabVIEW — recorded with
the generators and recipes in `dev/ni/`. `tests/test_reader.py` holds every
`.tdms` here to `expected_channels.json`, its channel contents as frozen when
the file was verified against NI's own software, and holds the DAQmx recordings
to `daqmx_first_samples.json`, the values DAQmx itself returned while logging.
`tests/test_recordings.py` runs the extractor end to end over all of them. Add
a row for each file.

| File | Made with | Exercises |
|------|-----------|-----------|
| `lv_incremental_props.tdms` | LabVIEW, `dev/ni/labview/tdms_gen_garbage.vi` | TDMS Set Properties between writes: LabVIEW folds the property changes into the next segment's metadata (every segment carries a new object list); a property overwritten by name; a group property |
| `lv_channel_subset.tdms` | LabVIEW | The written channel list shrinking, growing and reordering between segments; auto-named `Untitled N` channels; a group given no name |
| `lv_interleaved.tdms` | LabVIEW | `kTocInterleavedData` segments of int32 and of float64, one holding two interleaved chunks |
| `lv_types_le.tdms` / `lv_types_be.tdms` | LabVIEW, the second opened big-endian | Every fixed-width type, booleans, strings (empty and `°`), timestamp channels, one property of every type, in both byte orders; two channels declared with zero values; a channel name with quotes and a slash |
| `lv_waveforms.tdms` | LabVIEW | Waveform writes: `wf_start_time` with a sub-microsecond fraction, `wf_increment`; a segment holding two chunks |
| `lv_many_segments.tdms` | LabVIEW | 50 writes of the same channels: LabVIEW appends them to one segment as 49 repeated chunks |
| `lv_advanced_chunks.tdms` | LabVIEW Advanced TDMS API | Five synchronous writes into one segment, five chunks |
| `lv_defragmented.tdms` | LabVIEW, TDMS Defragment of `lv_many_segments.tdms` | The same content consolidated into one 40000-byte raw block |
| `daqmx_ai_i16.tdms` | DAQmx 26.3 logging, `dev/ni/make_daqmx_fixtures.py`, simulated PCIe-6363 | Four AI channels as int16 format-changing scalers in one 8-byte sample, linear scaling, five chunks per segment |
| `daqmx_ai_i32.tdms` | DAQmx, simulated PXI-6289 | 18-bit device: raw int32 scalers |
| `daqmx_ai_unsigned.tdms` | DAQmx, simulated USB-6009 | A low-cost USB device's raw encoding and scaling |
| `daqmx_ai_two_widths.tdms` | DAQmx, simulated cDAQ-9189 with NI 9205 and NI 9239 | Two raw buffers of different sample width in one task |
| `daqmx_di_lines.tdms` | DAQmx, PCIe-6363 port0 | Digital-line scalers: eight lines as bits of a 1-byte port sample; the `0x126A` index header |
| `daqmx_scale_polynomial.tdms` | DAQmx, custom polynomial scale `x + 0.05 x²` | A `Polynomial` stage fed by the Linear stage (a degree-1 polynomial would have been folded into the Linear stage instead) |
| `daqmx_scale_table.tdms` / `daqmx_scale_map_ranges.tdms` | DAQmx, custom table and map-ranges scales | A `Table` stage fed by the Linear stage: interpolation and clamping at the table's ends. DAQmx's own values show the stage's input runs along `Scaled_Values` and its output along `Pre_Scaled_Values`, the reverse of what the names suggest |
| `daqmx_thermocouple.tdms` | DAQmx, simulated NI 9213, K type | Two raw scalers (signal and cold junction), Linear, Subtract and `Thermocouple` stages. DAQmx read the first five samples as -8.856241, -10.631426, -9.478097, -7.964961, -8.574639 °C, which fixes the Subtract stage's operand order (right minus left) and checks the NIST inverse functions |
| `daqmx_log_only.tdms` | DAQmx `LoggingMode.LOG` | Logging without reads: one large segment |
| `daqmx_rtd_pt3851_4wire.tdms` / `daqmx_rtd_pt3750_3wire.tdms` | DAQmx, simulated NI 9217, Pt100 | `RTD` stage: Callendar-Van Dusen coefficients, excitation current, 4-wire and 3-wire configurations |
| `daqmx_rtd_pt1000.tdms` | DAQmx, simulated NI 9219, Pt1000 | `RTD` stage with R0 = 1000 Ω and 500 µA excitation |
| `daqmx_thermistor.tdms` | DAQmx, simulated PCIe-6363, external 2.5 V excitation, 5 kΩ reference | `Thermistor` stage (Steinhart-Hart on a voltage divider) followed by a Linear stage from kelvin to Celsius |
| `daqmx_strain_quarter.tdms` | DAQmx, simulated NI 9235 | `Strain` stage, quarter bridge I (code 10271), 120 Ω, 2 V excitation |
| `daqmx_strain_full.tdms` / `daqmx_strain_half.tdms` | DAQmx, simulated NI 9219 | `Strain` stage, full bridge I (10183) and half bridge I (10188), 350 Ω, 2.5 V |
| `daqmx_bridge_vv.tdms` | DAQmx, simulated NI 9219 | A bridge channel in V/V: plain Linear scaling, no bridge stage |
| `daqmx_strain_full2.tdms` / `daqmx_strain_full3.tdms` / `daqmx_strain_half2.tdms` | DAQmx, simulated NI 9219 | The other bridge configurations DAQmx offers there (codes 10184, 10185, 10189) |
| `daqmx_strain_quarter2.tdms` | DAQmx, simulated NI 9237 | Quarter bridge II (code 10272), which the fixed 9235 cannot be configured for |
| `daqmx_strain_quarter_leads.tdms` | DAQmx, simulated NI 9235 | Quarter bridge I with 1.5 Ω lead resistance, a 2 mV initial bridge voltage and a shunt-calibration gain of 1.05 |
| `daqmx_strain_full_offset.tdms` | DAQmx, simulated NI 9219 | Full bridge I with a 3 mV initial bridge voltage and a shunt-calibration gain of 0.98 |
| `daqmx_thermocouple_{B,E,J,N,R,S,T}.tdms` | DAQmx, simulated NI 9213 | Every other thermocouple type. The simulated millivolt signal is far below the valid range of types B, R and S, so their polynomials extrapolate to enormous negative temperatures — and DAQmx's own values do exactly the same |
| `daqmx_force_two_point.tdms` / `daqmx_pressure_table.tdms` / `daqmx_torque_polynomial.tdms` | DAQmx, simulated NI 9219 | Bridge-based sensors in physical units. No stage type of their own: two-point linear folds into the Linear stage, the others add a Table or Polynomial stage over the mV/V reading |
| `lv_ext.tdms` | LabVIEW, `dev/ni/labview/tdms_gen_garbage.vi` | An EXT channel and an EXT property: 80-bit x87 values stored as 10 bytes, no padding. Known values (1.0, -2.5, 1e100, π, 0.1, -0.0, 1e-300) pin the layout |

`daqmx_first_samples.json` holds the first five values of each recording's
first channel exactly as DAQmx returned them to the recording script, rounded
to six decimals. `tests/test_reader.py` requires the reader to reproduce them,
which checks the scaling math against the device driver itself rather than
against another reader. Regenerating a recording means updating its row there
from the script's output.
