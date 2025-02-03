#!/usr/bin/env python3
"""Script to compile protobuf files."""

from pathlib import Path
import subprocess
import sys
from typing import Any, Dict

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class ProtosBuildHook(BuildHookInterface):
    """Build hook for compiling protobuf files."""
    
    def initialize(self, version: str, build_data: Dict[str, Any]) -> None:
        """Initialize the build hook."""
        proto_dir = Path("nominal/protos")
        protos = list(proto_dir.glob("*.proto"))
        
        if not protos:
            print("No .proto files found")
            return
        
        try:
            for proto in protos:
                subprocess.run([
                    "python", "-m", "grpc_tools.protoc",
                    f"--proto_path={proto_dir}",
                    f"--python_out={proto_dir}",
                    str(proto)
                ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error compiling protos: {e}", file=sys.stderr)
            raise


def build_hook(root: str) -> BuildHookInterface:
    """Return an instance of the build hook."""
    return ProtosBuildHook(root) 
