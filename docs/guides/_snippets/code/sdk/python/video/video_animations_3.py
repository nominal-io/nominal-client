import numpy as np
import pandas as pd
from scipy.integrate import solve_ivp
from datetime import datetime, timedelta

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
