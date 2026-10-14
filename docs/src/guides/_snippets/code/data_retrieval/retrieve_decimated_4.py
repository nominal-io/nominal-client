import plotly.graph_objects as go

if "value" in df_decimated.columns:
    # range had less than 1000 points
    fig = go.Figure(
        [
            go.Scatter(
                x=df_decimated.index,
                y=df_decimated["value"],
                mode="lines",
                # line=dict(width=0),
                showlegend=False,
                # line_shape="vh"
            ),
        ]
    )
else:
    fig = go.Figure(
        [
            go.Scatter(
                x=df_decimated.index,
                y=df_decimated["max"],
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                line_shape="vh",
            ),
            go.Scatter(
                x=df_decimated.index,
                y=df_decimated["min"],
                mode="lines",
                fill="tonexty",
                fillcolor="blue",
                line=dict(width=0),
                showlegend=False,
                line_shape="vh",
            ),
        ]
    )

fig.update_layout(xaxis_title="Time", yaxis_title="Value")

fig.show()
