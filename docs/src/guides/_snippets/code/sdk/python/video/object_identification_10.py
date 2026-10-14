computer_vision_run = client.create_run(
    name="RT-DETR model analysis",
    start=df_computer_vision["timestamps"].min(),
    end=df_computer_vision["timestamps"].max(),
    description="Run analysis of RT-DETR model output on single drone flight footage.",
)

computer_vision_run
