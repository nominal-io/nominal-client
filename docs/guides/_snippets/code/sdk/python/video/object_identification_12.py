import torch
from PIL import Image
from transformers import RTDetrForObjectDetection, RTDetrImageProcessor
import polars as pl


def get_objects_from_pil_image(image, frame):
    """
    This function takes an image in PIL format and returns a Polars dataframe with all of the identified objects
    """
    schema = {
        "frame": pl.Int64,  # Column 'frame' as integer
        "object": pl.Utf8,  # Column 'object' as string
        "score": pl.Float64,  # Column 'score' as float
        "x_min": pl.Float64,  # Column 'x_min' as integer
        "y_min": pl.Float64,  # Column 'y_min' as integer
        "x_max": pl.Float64,  # Column 'x_max' as integer
        "y_max": pl.Float64,  # Column 'y_max' as integer
    }
    df_video_frame = pl.DataFrame(schema=schema)

    image_processor = RTDetrImageProcessor.from_pretrained("PekingU/rtdetr_r50vd")
    model = RTDetrForObjectDetection.from_pretrained("PekingU/rtdetr_r50vd")

    inputs = image_processor(images=image, return_tensors="pt")

    with torch.no_grad():
        outputs = model(**inputs)

    results = image_processor.post_process_object_detection(
        outputs, target_sizes=torch.tensor([image.size[::-1]]), threshold=0.3
    )

    for result in results:
        for score, label_id, box in zip(
            result["scores"], result["labels"], result["boxes"]
        ):
            score, label = score.item(), label_id.item()
            box = [round(i, 2) for i in box.tolist()]
            new_row = pl.DataFrame(
                {
                    "frame": [frame],
                    "object": [model.config.id2label[label]],
                    "score": [score],
                    "x_min": [box[0]],
                    "y_min": [box[1]],
                    "x_max": [box[2]],
                    "y_max": [box[3]],
                }
            )
            df_video_frame = pl.concat([df_video_frame, new_row])

    return df_video_frame
