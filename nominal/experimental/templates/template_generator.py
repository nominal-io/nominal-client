from typing import Any, TextIO, Union

import yaml

from nominal.core.client import NominalClient
from nominal.core.workbook_template import WorkbookTemplate
from nominal.experimental.templates.raw_template import RawTemplate


class TemplateGenerator:
    def __init__(self, client: NominalClient):
        """Define a template generator object"""
        self.client = client

    """Helper functions for parsing yaml into RawTemplate"""

    def _validate_overall_structure(self, data: Any) -> None:
        # Validate required top-level fields
        required_fields = ["version", "title", "tabs"]
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Could not find '{field}' in yaml. See docs for structure")

        # Validate field types
        if not isinstance(data["version"], int):
            raise ValueError("'version' must be an integer")
        if not isinstance(data["title"], str) or not data["title"].strip():
            raise ValueError("'title' must be a non-empty string")
        if not isinstance(data["tabs"], dict) or len(data["tabs"]) == 0:
            raise ValueError("'tabs' must be a non-empty dictionary")

        # Validate labels if present
        if "labels" in data:
            if not isinstance(data["labels"], list):
                raise ValueError("'labels' must be a list")

    def _safe_open(self, yaml_input: Union[str, TextIO]) -> Any:
        """Open either a file object or a filepath"""
        try:
            if isinstance(yaml_input, str):
                with open(yaml_input, "r") as file:
                    data = yaml.safe_load(file)
            else:
                data = yaml.safe_load(yaml_input)
        except Exception as e:
            raise IOError(f"Error opening file: {e}")

        self._validate_overall_structure(data)
        return data

    def create_template_from_yaml(
        self, yaml_input: Union[str, TextIO], refname: str, commit_message: str
    ) -> WorkbookTemplate:
        """Main user facing function for creating template.
        TODO: currently creates a new template every call. an updating mechanism would be useful
        """
        try:
            data = self._safe_open(yaml_input)
            template = RawTemplate(data, refname)
        except Exception as e:
            raise ValueError(f"Error parsing template: {e}")

        try:
            template_request = template.create_request(commit_message)
            conjure_template = self.client._clients.template.create(self.client._clients.auth_header, template_request)
            return WorkbookTemplate._from_conjure(self.client._clients, conjure_template)
        except Exception as e:
            raise ValueError(f"Error creating template: {e}")
