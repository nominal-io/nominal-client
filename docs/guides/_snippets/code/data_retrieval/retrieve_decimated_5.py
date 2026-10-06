def on_range_change(layout, range_x):
    start = parser.isoparse(range_x[0] + "Z")
    end = parser.isoparse(range_x[1] + "Z")

    df = channel_to_dataframe_decimated(channel, start, end, buckets=2000)

    # Create a new figure with the new data
    fig = make_fig(df)
    fig.update_layout(xaxis={"range": range_x})

    # Update the existing figure widget with the new figure
    fig_widget.layout = fig.layout
    fig_widget.layout.on_change(on_range_change, "xaxis.range")
    fig_widget.data = []
    fig_widget.add_traces(fig.data)


def make_fig(df):
    if "value" in df.columns:
        # range had less than 1000 points
        fig = go.Figure(
            [
                go.Scatter(
                    x=df.index,
                    y=df["value"],
                    mode="lines",
                    showlegend=False,
                ),
            ]
        )
    else:
        fig = go.Figure(
            [
                go.Scatter(
                    x=df.index,
                    y=df["max"],
                    mode="lines",
                    line=dict(width=0),
                    showlegend=False,
                    line_shape="vh",
                ),
                go.Scatter(
                    x=df.index,
                    y=df["min"],
                    mode="lines",
                    fill="tonexty",
                    fillcolor="blue",
                    line=dict(width=0),
                    showlegend=False,
                    line_shape="vh",
                ),
            ]
        )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Value",
    )
    return fig


fig_range = make_fig(channel_to_dataframe_decimated(channel, start, end, buckets=2000))
fig_widget = go.FigureWidget(fig_range)

fig_widget.layout.on_change(on_range_change, "xaxis.range")

fig_widget
