object_count_data = dict(
    frame=[],
    total_object_count=[],
    motorbike_count=[],
    car_count=[],
    person_count=[],
    boat_count=[],
    bus_count=[],
    truck_count=[],
)

for frame_count in range(df_video_with_timestamps["frame"].max()):
    df_single_frame = df_video_with_timestamps.filter(pl.col("frame") == frame_count)
    object_count_data["frame"].append(frame_count)
    object_count_data["total_object_count"].append(len(df_single_frame))
    object_count_data["motorbike_count"].append(
        len(df_single_frame.filter(pl.col("object") == "motorbike"))
    )
    object_count_data["car_count"].append(
        len(df_single_frame.filter(pl.col("object") == "car"))
    )
    object_count_data["person_count"].append(
        len(df_single_frame.filter(pl.col("object") == "person"))
    )
    object_count_data["boat_count"].append(
        len(df_single_frame.filter(pl.col("object") == "boat"))
    )
    object_count_data["bus_count"].append(
        len(df_single_frame.filter(pl.col("object") == "bus"))
    )
    object_count_data["truck_count"].append(
        len(df_single_frame.filter(pl.col("object") == "truck"))
    )

df_object_count = pl.DataFrame(object_count_data)
df_video_w_object_count = df_video_with_timestamps.join(df_object_count, on="frame")

df_video_w_object_count.head()
