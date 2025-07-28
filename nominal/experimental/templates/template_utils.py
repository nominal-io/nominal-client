import re
from dataclasses import dataclass

"""Aliases for timeseries template objects"""
TemplateAxis = tuple[str, str]  # (axis title, axis side (0=R/1=L))
TemplateRow = dict[str, tuple[str, TemplateAxis]]  # {channel name: (color, TemplateAxis)}

"""Alias for scatter plot objects"""
TemplatePlot = TemplateRow


def validate_hex_color(color: str) -> bool:
    """Validate that a color string is in valid hex format (#RRGGBB)"""
    if not isinstance(color, str):
        return False
    return bool(re.match(r'^#[0-9A-Fa-f]{6}$', color))


@dataclass
class Comparisons:
    """Comparison run objects"""

    name: str
    color: str
    run_rids: list[str]
