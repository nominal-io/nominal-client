---
myst:
  html_meta:
    description: "Save and upload chart animations as video with Python and Nominal"
---

# Save plot animations as video in Python

{.lead}
Save and upload chart animations as video with Python and Nominal

For time-resolved simulations, you may want to save a dynamic (animated) plot
as a video so that it can be played back in sync with other simulation variables.
This guide describes how to accomplish this with the "double pendulum simulation" as an example.

```{include} /guides/_snippets/about-nominal-video.md
```

This guide demonstrates how to upload a video file to Nominal in Python.
Other guides in the Video section demonstrate basic Python video analysis
and computer vision for video pre-processing prior to Nominal upload.

## Double pendulum simulation

The double pendulum is a classic physics problem: two pendulums attached end to end, where
the motion of one pendulum influences the other, creating complex, chaotic dynamics. The
system demonstrates how even simple systems can exhibit unpredictable behavior due to
sensitivity to initial conditions.

If you've never seen the double pendulum simulation, below is a time-resolved animation of its solution.
Note that the simulation length is 25 seconds.

```python
import numpy as np
import plotly.graph_objs as go
from scipy.integrate import solve_ivp

# Parameters for the double pendulum
m1, m2 = 1.0, 1.0  # masses of the pendulums
L1, L2 = 1.0, 1.0  # lengths of the pendulums
g = 9.81  # gravitational acceleration


# Equations of motion for the double pendulum
def double_pendulum_equations(t, y):
    theta1, z1, theta2, z2 = y
    delta = theta2 - theta1

    # Equations of motion
    den1 = (m1 + m2) * L1 - m2 * L1 * np.cos(delta) * np.cos(delta)
    den2 = (L2 / L1) * den1

    dydt = [
        z1,
        (
            m2 * L1 * z1 * z1 * np.sin(delta) * np.cos(delta)
            + m2 * g * np.sin(theta2) * np.cos(delta)
            + m2 * L2 * z2 * z2 * np.sin(delta)
            - (m1 + m2) * g * np.sin(theta1)
        )
        / den1,
        z2,
        (
            -m2 * L2 * z2 * z2 * np.sin(delta) * np.cos(delta)
            + (m1 + m2)
            * (
                g * np.sin(theta1) * np.cos(delta)
                - L1 * z1 * z1 * np.sin(delta)
                - g * np.sin(theta2)
            )
        )
        / den2,
    ]
    return dydt


# Initial conditions and time span
y0 = [np.pi / 2, 0, np.pi / 2, 0]  # initial angles and angular velocities
t_span = (0, 25)
t_eval = np.linspace(*t_span, 500)

# Solve the equations
solution = solve_ivp(
    double_pendulum_equations, t_span, y0, t_eval=t_eval, method="RK45"
)
theta1, theta2 = solution.y[0], solution.y[2]

# Calculate positions of the pendulums
x1 = L1 * np.sin(theta1)
y1 = -L1 * np.cos(theta1)
x2 = x1 + L2 * np.sin(theta2)
y2 = y1 - L2 * np.cos(theta2)

# Create animation using plotly
frames = []
for i in range(len(t_eval)):
    frames.append(
        go.Frame(
            data=[
                go.Scatter(
                    x=[0, x1[i], x2[i]], y=[0, y1[i], y2[i]], mode="lines+markers"
                )
            ]
        )
    )

# Set up the initial plot
fig = go.Figure(
    data=[go.Scatter(x=[0, x1[0], x2[0]], y=[0, y1[0], y2[0]], mode="lines+markers")],
    layout=go.Layout(
        title="Double Pendulum Animation",
        xaxis=dict(range=[-2, 2], zeroline=False),
        yaxis=dict(range=[-2, 2], zeroline=False),
        updatemenus=[
            dict(
                type="buttons",
                showactive=False,
                buttons=[
                    dict(
                        label="Play",
                        method="animate",
                        args=[
                            None,
                            {
                                "frame": {"duration": 50, "redraw": True},
                                "fromcurrent": True,
                            },
                        ],
                    )
                ],
            )
        ],
        width=600,
        height=600,
    ),
    frames=frames,
)

# Show the animation
fig.show()
```

```{video} /guides/images/b2/CleanShot_2024-10-17_at_19.05.06_nnna1m.mp4
:loop:
:alt: double-pendulum
```

## Convert animation to video

To convert the Plotly animation to a video, install the `imageio` library with the `ffmpeg` plugin:

`pip3 install imageio[ffmpeg]`

Prior to running the above command, you'll also want to install the underlying `ffmpeg` library.
On a Mac you can install this with `brew install ffmpeg`. For installation instructions for
other operating systems, please see the official [FFmpeg download page](https://www.ffmpeg.org/download.html).

For more information about this plugin, please refer to the
[imageio docs](https://imageio.readthedocs.io/en/v2.10.0/reference/_backends/imageio.plugins.ffmpeg.html).

Now modify the simulation code above to save an image for each animation frame
with Plotly's `write_image()` function. At the end of the simulation, stitch the
images together into a video with imageio's
[`get_writer()`](https://imageio.readthedocs.io/en/v2.9.0/userapi.html#imageio.get_writer) function.

```python
import os

import imageio.v2 as imageio
import numpy as np
import plotly.graph_objs as go
from scipy.integrate import solve_ivp

# Parameters for the double pendulum
m1, m2 = 1.0, 1.0
L1, L2 = 1.0, 1.0
g = 9.81


# Equations of motion
def double_pendulum_equations(t, y):
    theta1, z1, theta2, z2 = y
    delta = theta2 - theta1

    den1 = (m1 + m2) * L1 - m2 * L1 * np.cos(delta) ** 2
    den2 = (L2 / L1) * den1

    dydt = [
        z1,
        (
            m2 * L1 * z1**2 * np.sin(delta) * np.cos(delta)
            + m2 * g * np.sin(theta2) * np.cos(delta)
            + m2 * L2 * z2**2 * np.sin(delta)
            - (m1 + m2) * g * np.sin(theta1)
        )
        / den1,
        z2,
        (
            -m2 * L2 * z2**2 * np.sin(delta) * np.cos(delta)
            + (m1 + m2)
            * (
                g * np.sin(theta1) * np.cos(delta)
                - L1 * z1**2 * np.sin(delta)
                - g * np.sin(theta2)
            )
        )
        / den2,
    ]
    return dydt


# Initial conditions and time span
y0 = [np.pi / 2, 0, np.pi / 2, 0]
t_span = (0, 25)
t_eval = np.linspace(*t_span, 500)

# Solve the equations
solution = solve_ivp(
    double_pendulum_equations, t_span, y0, t_eval=t_eval, method="RK45"
)
theta1, theta2 = solution.y[0], solution.y[2]

# Calculate positions
x1 = L1 * np.sin(theta1)
y1 = -L1 * np.cos(theta1)
x2 = x1 + L2 * np.sin(theta2)
y2 = y1 - L2 * np.cos(theta2)

# Save frames as images
filenames = []
for i in range(len(t_eval)):
    fig = go.Figure(
        data=[
            go.Scatter(x=[0, x1[i], x2[i]], y=[0, y1[i], y2[i]], mode="lines+markers")
        ],
        layout=go.Layout(
            title="Double Pendulum Animation",
            xaxis=dict(range=[-2, 2], zeroline=False),
            yaxis=dict(range=[-2, 2], zeroline=False),
            width=600,
            height=600,
        ),
    )
    filename = f"frame_{i:03d}.png"
    fig.write_image(filename)
    filenames.append(filename)

# Create video from frames using imageio
with imageio.get_writer("double_pendulum.mp4", fps=20) as writer:
    for filename in filenames:
        image = imageio.imread(filename)
        writer.append_data(image)

# Cleanup
for filename in filenames:
    os.remove(filename)

print("Video saved as 'double_pendulum.mp4'")
```

Note that the simulation video has been saved to *double_pendulum.mp4*.

## Display video inline

```{include} /guides/_snippets/inline-video.md
```

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1729218249/double_pendulum_ozyjtj.mp4
```

## Save simulation channels

Run the same simulation a final time to extract channels such as the pendulum
fulcrum's heights, momentums, PEs and KEs.

:::{note}

Simulations are often time-consuming to run. In practice, extract the
simulation result, video frames, and important channel values in a single pass. For the pedagogical
purposes of this tutorial, the extraction of these 3 artifacts is separated into 3
identical but separate simulation runs.
:::

```python
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy.integrate import solve_ivp

# Parameters for the double pendulum
m1, m2 = 1.0, 1.0  # masses of the pendulums
L1, L2 = 1.0, 1.0  # lengths of the pendulums
g = 9.81  # gravitational acceleration


# Equations of motion for the double pendulum
def double_pendulum_equations(t, y):
    theta1, z1, theta2, z2 = y
    delta = theta2 - theta1

    # Equations of motion
    den1 = (m1 + m2) * L1 - m2 * L1 * np.cos(delta) * np.cos(delta)
    den2 = (L2 / L1) * den1

    dydt = [
        z1,
        (
            m2 * L1 * z1 * z1 * np.sin(delta) * np.cos(delta)
            + m2 * g * np.sin(theta2) * np.cos(delta)
            + m2 * L2 * z2 * z2 * np.sin(delta)
            - (m1 + m2) * g * np.sin(theta1)
        )
        / den1,
        z2,
        (
            -m2 * L2 * z2 * z2 * np.sin(delta) * np.cos(delta)
            + (m1 + m2)
            * (
                g * np.sin(theta1) * np.cos(delta)
                - L1 * z1 * z1 * np.sin(delta)
                - g * np.sin(theta2)
            )
        )
        / den2,
    ]
    return dydt


# Initial conditions and time span
y0 = [np.pi / 2, 0, np.pi / 2, 0]  # initial angles and angular velocities
t_span = (0, 25)
t_eval = np.linspace(*t_span, 500)

# Solve the equations
solution = solve_ivp(
    double_pendulum_equations, t_span, y0, t_eval=t_eval, method="RK45"
)
theta1, theta2 = solution.y[0], solution.y[2]

# Calculate positions, velocities, accelerations, and energies
x1 = L1 * np.sin(theta1)
y1 = -L1 * np.cos(theta1)
x2 = x1 + L2 * np.sin(theta2)
y2 = y1 - L2 * np.cos(theta2)

vx1 = np.gradient(x1, t_eval)
vy1 = np.gradient(y1, t_eval)
vx2 = np.gradient(x2, t_eval)
vy2 = np.gradient(y2, t_eval)

ax1 = np.gradient(vx1, t_eval)
ay1 = np.gradient(vy1, t_eval)
ax2 = np.gradient(vx2, t_eval)
ay2 = np.gradient(vy2, t_eval)

momentum_1_x = m1 * vx1
momentum_1_y = m1 * vy1
momentum_2_x = m2 * vx2
momentum_2_y = m2 * vy2

height_1 = y1 + L1 + L2
height_2 = y2 + L1 + L2

kinetic_energy_1 = 0.5 * m1 * (vx1**2 + vy1**2)
kinetic_energy_2 = 0.5 * m2 * (vx2**2 + vy2**2)

potential_energy_1 = m1 * g * (y1 + L1 + L2)
potential_energy_2 = m2 * g * (y2 + L1 + L2)

# Generate ISO8601 formatted timestamps
start_time = datetime.now()
absolute_time = [start_time + timedelta(seconds=t) for t in t_eval]

# Create DataFrame with full header names
data = {
    "Timestamp (ISO8601)": [t.isoformat() for t in absolute_time],
    "Momentum 1 (X)": momentum_1_x,
    "Momentum 1 (Y)": momentum_1_y,
    "Momentum 2 (X)": momentum_2_x,
    "Momentum 2 (Y)": momentum_2_y,
    "Acceleration 1 (X)": ax1,
    "Acceleration 1 (Y)": ay1,
    "Acceleration 2 (X)": ax2,
    "Acceleration 2 (Y)": ay2,
    "Velocity 1 (X)": vx1,
    "Velocity 1 (Y)": vy1,
    "Velocity 2 (X)": vx2,
    "Velocity 2 (Y)": vy2,
    "Height 1": height_1,
    "Height 2": height_2,
    "Kinetic Energy 1": kinetic_energy_1,
    "Kinetic Energy 2": kinetic_energy_2,
    "Potential Energy 1": potential_energy_1,
    "Potential Energy 2": potential_energy_2,
}

df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
csv_file_path_full_headers = "double_pendulum_full_headers.csv"
df.to_csv(csv_file_path_full_headers, index=False)

df
```

![simulation-dataframe](/guides/images/b2/8b65a3ab-469e-407f-bdb9-5fa2a6a6a900.png)

The channel data is saved in a CSV file: *double_pendulum_full_headers.csv*.

Finally, upload the channels and video to Nominal so they can be visualized together.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create a run

Start by creating a run to attach the video and channel data to.

```{include} /guides/_snippets/what-is-a-run.md
```

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

csv_dataset = client.create_dataset("double pendulum")
csv_dataset_file = csv_dataset.add_tabular_data(
    path="double_pendulum_full_headers.csv",
    timestamp_column="Timestamp (ISO8601)",
    timestamp_type="iso_8601",
)
csv_dataset_file.poll_until_ingestion_completed()
csv_dataset.refresh()

# Now create a run and attach the data to it

sim_run = client.create_run(
    name="double_pendulum_run",
    start=csv_dataset.bounds.start,
    end=csv_dataset.bounds.end,
)
sim_run.add_dataset(ref_name="measurements", dataset=csv_dataset)
```

## Add video to the dataset

Nominal video uploads need an absolute start time. Grab this start time from the channel dataframe:

```python
ts_start = df["Timestamp (ISO8601)"][0]
ts_start
```
`2024-10-16T22:47:54.600400`

Upload the video as a channel on the same dataset that holds the pendulum measurements:

```python
video_file = csv_dataset.add_video(
    path="double_pendulum.mp4",
    channel="pendulum_video",
    start=ts_start,
)
```

That dataset is already attached to the run, so the new video channel appears there as soon as it
finishes ingesting:

```python
video_file.poll_until_ingestion_completed()
```

## Adjust Video Start

Nominal has many tools to precisely sync video playback with channel data, which is typically
extracted from the video or recorded independently at the same time.

If you select the dataset on the [datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets)
(login required), then you can adjust metadata such as a video file's start timestamp. In Python,
use `VideoDatasetFile.update(starting_timestamp=...)` on a file from `Dataset.list_video_files()`.

![video-start-date](/guides/images/b2/31f8a27b-ba94-45fb-803b-92ffd524a814.png)

## Create a workbook

The simulation video and channel data are now uploaded on the Nominal platform,
where they can be collaboratively analyzed and benchmarked against real-world data.

Below is an example Nominal workbook that syncs the pendulum fulcrum heights with the pendulum simulation video playback:

```{video} /guides/images/b2/CleanShot_2024-10-17_at_09.12.40_hoivoh.mp4
:loop:
:alt: simulation-workbook
```

Other channels such as pendulum fulcrum momentum, acceleration, and kinetic energy can also be plotted. Workbook transforms and formulas
can be used to calculate derived channels such as the RMS acceleration.

As an exercise, consider solving the simulation with a different ODE solver and comparing the results in a Nominal workbook.
