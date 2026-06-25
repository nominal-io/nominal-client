#!/usr/bin/env python3
"""Extract a normalized API surface from an unpacked nominal bindings wheel.

Two transports:
  conjure  -> nominal-api        (parses nominal_api/_impl.py)
  proto    -> nominal-api-protos (parses nominal/protos/**/*_pb2.pyi + *_pb2_grpc.py)

Usage:
  extract_surface.py conjure <root>   # <root> contains nominal_api/_impl.py
  extract_surface.py proto   <root>   # <root> contains nominal/protos/...

Emits one normalized, sorted line per surface element. The grammar is shared so
the two versions of a package can be diffed by diff_surface.py:

  [bean]    <module>.<Name>(field: Type, field: Type = ..., ...)
  [union]   <module>.<Name>{ field: Type = ..., ... }
  [enum]    <module>.<Name>: {MEMBER, MEMBER, ...}
  [service] <module>.<Name>.<method>(arg, ...) -> Ret
  [message] <module>.<Name>(field: Type = ..., ...)        # proto
  [rpc]     <module>.<Service>.<Method>                    # proto grpc

`= ...` marks an optional field (has a default). Type strings are kept fully
qualified so a type change (e.g. List->Dict) is detectable; the report shortens
them for display.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

# raw conjure symbol `scout_compute_resolved_api_ArithmeticSeriesNode`
# -> module `scout_compute_resolved_api`, name `ArithmeticSeriesNode`
_CONJURE_NAME = re.compile(r"^([a-z0-9_]+?)_([A-Z]\w*)$")


def _ann(node: ast.expr | None) -> str:
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:
        return "?"


def _base_attrs(cls: ast.ClassDef) -> set[str]:
    out: set[str] = set()
    for b in cls.bases:
        if isinstance(b, ast.Name):
            out.add(b.id)
        elif isinstance(b, ast.Attribute):
            out.add(b.attr)
    return out


def _init_fields(cls: ast.ClassDef) -> list[str]:
    """`name: Type` / `name: Type = ...` for each __init__ param (excludes self)."""
    for item in cls.body:
        if isinstance(item, ast.FunctionDef) and item.name == "__init__":
            args = item.args
            has_default = {
                len(args.args) - len(args.defaults) + i for i in range(len(args.defaults))
            }
            fields = []
            for i, a in enumerate(args.args):
                if a.arg == "self":
                    continue
                opt = " = ..." if i in has_default else ""
                fields.append(f"{a.arg}: {_ann(a.annotation)}{opt}")
            return fields
    return []


# ---------------------------------------------------------------- conjure ----
def extract_conjure(root: Path) -> list[str]:
    impl = root / "nominal_api" / "_impl.py"
    tree = ast.parse(impl.read_text(encoding="utf-8"))
    lines: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        raw = node.name
        if raw.endswith("Visitor"):
            continue  # visitors mirror unions; the union row already captures the change
        m = _CONJURE_NAME.match(raw)
        qual = f"{m.group(1)}.{m.group(2)}" if m else raw
        bases = _base_attrs(node)
        if "Service" in bases:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and not item.name.startswith("_"):
                    params = [a.arg for a in item.args.args if a.arg != "self"]
                    ret = _ann(item.returns)
                    ret = f" -> {ret}" if ret else ""
                    lines.append(f"[service] {qual}.{item.name}({', '.join(params)}){ret}")
        elif "ConjureEnumType" in bases:
            members = sorted(
                t.id
                for it in node.body
                if isinstance(it, ast.Assign)
                for t in it.targets
                if isinstance(t, ast.Name) and t.id.isupper()
            )
            lines.append(f"[enum] {qual}: {{{', '.join(members)}}}")
        elif "ConjureUnionType" in bases:
            lines.append(f"[union] {qual}{{ {', '.join(_init_fields(node))} }}")
        elif "ConjureBeanType" in bases:
            lines.append(f"[bean] {qual}({', '.join(_init_fields(node))})")
    return sorted(lines)


# ------------------------------------------------------------------ proto ----
def _proto_module(pyi: Path, protos_root: Path) -> str:
    rel = pyi.relative_to(protos_root).parent  # e.g. file_store/v1
    return ".".join(rel.parts) if rel.parts else "<root>"


def _is_message(cls: ast.ClassDef) -> bool:
    return "Message" in _base_attrs(cls)


def _is_enum(cls: ast.ClassDef) -> bool:
    if any(isinstance(b, ast.Name) and b.id == "int" for b in cls.bases):
        return True
    return any(k.arg == "metaclass" for k in cls.keywords)


def extract_proto(root: Path) -> list[str]:
    protos_root = root / "nominal" / "protos"
    lines: list[str] = []
    for pyi in sorted(protos_root.rglob("*_pb2.pyi")):
        module = _proto_module(pyi, protos_root)
        tree = ast.parse(pyi.read_text(encoding="utf-8"))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            if _is_enum(node):
                members = sorted(
                    it.target.id
                    for it in node.body
                    if isinstance(it, ast.AnnAssign) and isinstance(it.target, ast.Name)
                )
                lines.append(f"[enum] {module}.{node.name}: {{{', '.join(members)}}}")
            elif _is_message(node):
                lines.append(f"[message] {module}.{node.name}({', '.join(_init_fields(node))})")
    for grpc in sorted(protos_root.rglob("*_pb2_grpc.py")):
        module = _proto_module(grpc, protos_root)
        tree = ast.parse(grpc.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name.endswith("Servicer"):
                service = node.name[: -len("Servicer")]
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and not item.name.startswith("_"):
                        lines.append(f"[rpc] {module}.{service}.{item.name}")
    return sorted(lines)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ("conjure", "proto"):
        sys.exit("usage: extract_surface.py {conjure|proto} <unpacked-wheel-root>")
    transport, root = sys.argv[1], Path(sys.argv[2])
    fn = extract_conjure if transport == "conjure" else extract_proto
    for line in fn(root):
        print(line)
