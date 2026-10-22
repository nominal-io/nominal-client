---
myst:
  html_meta:
    description: "Render a simulation as video and upload it with the simulation's channels, so they play back together."
---

# Chart animations

{.lead}
Render an animated chart of a simulation as a video, then upload it next to the simulation's channels so you can replay them together.
This example simulates a double pendulum.

The examples also need `pip install scipy matplotlib "imageio[ffmpeg]"`.

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1729218249/double_pendulum_ozyjtj.mp4
```

```{include} /guides/_snippets/connect-the-client.md
```

## Simulate and save the channels

Solve the equations of motion once, and save the channels with absolute timestamps:

```python
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from scipy.integrate import solve_ivp

g, L = 9.81, 1.0  # both arms 1 m long, both masses 1 kg


def pendulum(t, y):
    theta1, z1, theta2, z2 = y
    d = theta2 - theta1
    den = 2 * L - L * np.cos(d) ** 2
    dz1 = (L * z1**2 * np.sin(d) * np.cos(d) + g * np.sin(theta2) * np.cos(d) + L * z2**2 * np.sin(d) - 2 * g * np.sin(theta1)) / den
    dz2 = (-L * z2**2 * np.sin(d) * np.cos(d) + 2 * (g * np.sin(theta1) * np.cos(d) - L * z1**2 * np.sin(d) - g * np.sin(theta2))) / den
    return [z1, dz1, z2, dz2]


t = np.linspace(0, 25, 500)  # 25 s at 20 frames per second
theta1, _, theta2, _ = solve_ivp(pendulum, (0, 25), [np.pi / 2, 0, np.pi / 2, 0], t_eval=t).y
x1, y1 = L * np.sin(theta1), -L * np.cos(theta1)
x2, y2 = x1 + L * np.sin(theta2), y1 - L * np.cos(theta2)

start = datetime.now(timezone.utc)
pd.DataFrame(
    {
        "time": start.timestamp() + t,
        "height_1": y1 + 2 * L,
        "height_2": y2 + 2 * L,
        "speed_2": np.hypot(np.gradient(x2, t), np.gradient(y2, t)),
    }
).to_csv("pendulum.csv", index=False)
```

## Render the video

Draw one frame per time step and write them to an MP4 with [imageio](https://imageio.readthedocs.io/):

```python
import imageio.v2 as imageio
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(6, 6))
with imageio.get_writer("pendulum.mp4", fps=20) as writer:
    for i in range(len(t)):
        ax.clear()
        ax.set(xlim=(-2, 2), ylim=(-2, 2))
        ax.plot([0, x1[i], x2[i]], [0, y1[i], y2[i]], "o-")
        fig.canvas.draw()
        writer.append_data(np.asarray(fig.canvas.buffer_rgba())[..., :3])
```

The frame rate matches the simulation's time step, so the video lasts as long as the simulation.

## Upload the channels and the video

Upload both to one dataset. The video starts at the simulation's first timestamp:

```python
dataset = client.create_dataset("Double pendulum")
dataset.add_tabular_data("pendulum.csv", timestamp_column="time", timestamp_type="epoch_seconds").poll_until_ingestion_completed()
dataset.add_video("pendulum.mp4", channel="pendulum_video", start=start).poll_until_ingestion_completed()

run = client.create_run("Double pendulum", start=start, end=start + timedelta(seconds=25))
run.add_dataset("simulation", dataset)
print(run.nominal_url)
```

If the video drifts from the channels, change its start with `update(starting_timestamp=...)` on the file from {py:meth}`~nominal.core.Dataset.list_video_files`.
See [Video ingest](/guides/video/video-ingest.md) for other options.

## Replay it in a workbook

In a workbook on the run, add a video panel for `pendulum_video` and a chart of `height_1` and `height_2`.
Playing the video moves the chart's cursor with it.

```{video} /guides/images/b2/CleanShot_2024-10-17_at_09.12.40_hoivoh.mp4
:loop:
:alt: A workbook replaying the pendulum video next to its height channels
```
