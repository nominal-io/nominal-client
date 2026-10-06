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
