from nominal.core import LogPoint

logs = []
for i in range(len(df["source_time"])):
    log_point = LogPoint.create(
        df["source_time"][i],
        f"Log message {i} {generate_sparkline(random.randint(15, 40))}",
        None,
    )
    logs.append(log_point)

for log in logs[:5]:
    print((log.timestamp, log.message))
