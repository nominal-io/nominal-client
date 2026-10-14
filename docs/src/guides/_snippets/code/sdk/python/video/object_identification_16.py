df_object_count = (
    df_video_with_timestamps.group_by("frame")
    .agg(
        pl.len().alias("total_object_count"),
        (pl.col("object") == "motorbike").sum().alias("motorbike_count"),
        (pl.col("object") == "car").sum().alias("car_count"),
        (pl.col("object") == "person").sum().alias("person_count"),
        (pl.col("object") == "boat").sum().alias("boat_count"),
        (pl.col("object") == "bus").sum().alias("bus_count"),
        (pl.col("object") == "truck").sum().alias("truck_count"),
    )
    .sort("frame")
)
df_video_w_object_count = df_video_with_timestamps.join(df_object_count, on="frame")

df_video_w_object_count.head()
