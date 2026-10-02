from nominal.experimental.extractor import ManifestExtractorContext, manifest_extractor


@manifest_extractor
def extract(ctx: ManifestExtractorContext) -> None:
    frames = parse_bespoke_binary(ctx.input())

    # Parameters declared at registration arrive as strings. `param()` raises
    # if it is missing; `get_param()` takes a default.
    prefix = ctx.get_param("CHANNEL_PREFIX", "telemetry/")

    telemetry = ctx.output_dir / "flight_data.parquet"
    frames.telemetry.to_parquet(telemetry, index=False)
    ctx.add_tabular(
        telemetry,
        timestamp_column="timestamp",
        timestamp_type="epoch_microseconds",
        tag_columns={"vehicle_id": "veh_id"},
        channel_prefix=prefix,
    )

    logs = ctx.output_dir / "system_logs.jsonl"
    frames.logs.to_json(logs, orient="records", lines=True)
    ctx.add_journal_json(logs)

    # Video from the same run lands on a channel of the same dataset
    footage = ctx.output_dir / "nose_camera.mp4"
    frames.write_video(footage)
    ctx.add_video(footage, channel="nose_camera", start=frames.started_at)

    # manifest.json is written for you when this function returns


if __name__ == "__main__":
    extract.run()
