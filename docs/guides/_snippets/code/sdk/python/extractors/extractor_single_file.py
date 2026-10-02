import pandas as pd
from nominal.experimental.extractor import (
    SingleFileExtractorContext,
    single_file_extractor,
)


@single_file_extractor
def extract(ctx: SingleFileExtractorContext) -> None:
    # The mounted path for the image's declared input. With more than one
    # input, pass the environment variable name: ctx.input("INPUT_FILE").
    df = pd.read_csv(ctx.input())
    cleaned = df.dropna().reset_index(drop=True)

    output = ctx.output_dir / "processed_data.parquet"
    cleaned.to_parquet(output, index=False)

    # Declare the one file the pipeline should ingest. It is parsed according
    # to the output format the image was registered with.
    ctx.set_output(output)


if __name__ == "__main__":
    extract.run()
