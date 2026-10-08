from nominal.thirdparty.pandas import channel_to_dataframe_decimated

channel = dataset.get_channel("value")
df_decimated = channel_to_dataframe_decimated(channel, start, end, buckets=2000)
df_decimated
