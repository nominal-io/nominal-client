from nominal.core import NominalClient

client = NominalClient.from_profile("default")

template = client.get_workbook_template("your_template_rid")
template.create_workbook(
    run="your_run_rid",
    title="My new workbook",
    description="This is a new workbook created from a template",
)
