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
