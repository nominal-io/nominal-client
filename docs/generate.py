import inspect
from copy import deepcopy
from pathlib import Path
import griffe2md.main
import griffe2md.rendering


class LyingLineLength(int):
    """Unnecessarily ridiculous hack to fix some formatting inconsistencies with griffe2md.
    Ref: https://github.com/mkdocstrings/griffe2md/issues/1

    Could (should) have just made a fork of griffe2md and fixed it there, or just patch the functions, but this is more fun.

    If within griffe2md.rendering._format_signature() or griffe2md.rendering.do_format_code(), then
        len(code) < line_length
    will be False, to force griffe2md to use always use black to format the code.
    """

    def __gt__(self, other):
        for finfo in inspect.stack():
            if finfo.function in ["_format_signature", "do_format_code"]:
                # if we're in one of these functions, return False so that `black` is _always_ used to format code.
                return False
        return super().__gt__(other)


# These options are primarily documented in mkdocstrings-python's documentation:
# https://mkdocstrings.github.io/python/usage/configuration/general/
config = deepcopy(griffe2md.rendering.default_config)
config["docstring_options"] = {"ignore_init_summary": True}
config["docstring_section_style"] = "list"
config["filters"] = ["!^_"]
config["heading_level"] = 1
config["inherited_members"] = True
config["merge_init_into_class"] = True
config["show_signature_annotations"] = True
config["separate_signature"] = True
config["signature_crossrefs"] = False
config["group_by_category"] = True
config["show_object_full_path"] = False
config["line_length"] = LyingLineLength(72)
config["show_submodules"] = False
# some of the summary sections would be nice, but the links don't necessarily work outside of mkdocs:
# griffe will generate links like .#nominal.core.Run.add_dataset,
# but other md -> html frameworks may generate links like #nominalcorerunadd_dataset.
config["summary"] = False

# we deepcopy the configs each time because they get mutated, which prevents reuse.
output = Path(__file__).parent / "reference"
griffe2md.main.write_package_docs("nominal", config=deepcopy(config), output=str(output / "toplevel.md"))
griffe2md.main.write_package_docs("nominal.core", config=deepcopy(config), output=str(output / "core.md"))
griffe2md.main.write_package_docs("nominal.exceptions", config=deepcopy(config), output=str(output / "exceptions.md"))
