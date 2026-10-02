filepath = "/path/to/my_report.pdf"
with open(filepath, "rb") as f:
    attachment = client.create_attachment_from_io(f, "KittyHawk.pdf")
nominal_asset.add_attachments(attachments=[attachment])
