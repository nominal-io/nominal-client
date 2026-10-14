filepath = "/path/to/my_report.pdf"
with open(filepath, "rb") as f:
    attachment = client.create_attachment_from_io(
        f,
        name="KittyHawk.pdf",
    )
my_run.add_attachments(attachments=[attachment])
