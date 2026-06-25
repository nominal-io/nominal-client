#!/usr/bin/env python3
"""Extract a structured API surface from an unpacked nominal bindings wheel.

Two transports:
  conjure  -> nominal-api        (parses nominal_api/_impl.py)
  proto    -> nominal-api-protos (parses nominal/protos/**/*_pb2.pyi + *_pb2_grpc.py)

`extract(transport, root)` returns a list of `Element` records — the canonical,
structured surface. diff_surface and bump_scan consume these directly. The CLI
emits the same records as JSON Lines (one element per line, sorted by key) for
the standalone `extract | diff` workflow:

  extract_surface.py conjure <root>   # <root> contains nominal_api/_impl.py
  extract_surface.py proto   <root>   # <root> contains nominal/protos/...

Each Element is a bean / union / enum / message (fields), an enum (members), or a
service endpoint / rpc (params + return). Types are kept fully qualified so a type
change (e.g. List->Dict) is detectable; the report shortens them for display.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

# raw conjure symbol `scout_compute_resolved_api_ArithmeticSeriesNode`
# -> module `scout_compute_resolved_api`, name `ArithmeticSeriesNode`
_CONJURE_NAME = re.compile(r"^([a-z0-9_]+?)_([A-Z]\w*)$")


def use_utf8_stdout() -> None:
    """Make stdout UTF-8 so reports with ✅/⚠️/→/− print on any platform (e.g. Windows cp1252)."""
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    optional: bool


@dataclass
class Element:
    kind: str                          # bean | union | message | enum | service | rpc
    module: str
    qualname: str                      # leaf id; service endpoints use "Service.method"
    fields: list[Field] = field(default_factory=list)   # bean | union | message
    members: list[str] = field(default_factory=list)     # enum
    params: list[str] = field(default_factory=list)      # service
    returns: str = ""                                    # service

    @property
    def key(self) -> str:
        return f"{self.kind} {self.module}.{self.qualname}" if self.module else f"{self.kind} {self.qualname}"

    @property
    def display(self) -> str:
        return f"{self.module}.{self.qualname}" if self.module else self.qualname

    @property
    def leaf(self) -> str:
        return self.qualname.split(".")[-1]


def to_jsonl(elements: list[Element]) -> str:
    return "\n".join(json.dumps(asdict(e), sort_keys=True) for e in sorted(elements, key=lambda e: e.key))


def from_jsonl(text: str) -> list[Element]:
    out: list[Element] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        d = json.loads(line)
        out.append(
            Element(
                kind=d["kind"],
                module=d["module"],
                qualname=d["qualname"],
                fields=[Field(**f) for f in d.get("fields", [])],
                members=d.get("members", []),
                params=d.get("params", []),
                returns=d.get("returns", ""),
            )
        )
    return out


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


def _init_fields(cls: ast.ClassDef) -> list[Field]:
    """Fields from the __init__ signature (excludes self), positional and keyword-only.

    Newer mypy-protobuf stubs declare proto fields as keyword-only (`def __init__(self, *, ...)`),
    so both arg lists are read; a field is optional when it has a default.
    """
    for item in cls.body:
        if isinstance(item, ast.FunctionDef) and item.name == "__init__":
            a = item.args
            pos_defaulted = {len(a.args) - len(a.defaults) + i for i in range(len(a.defaults))}
            fields = [
                Field(arg.arg, _ann(arg.annotation), i in pos_defaulted)
                for i, arg in enumerate(a.args)
                if arg.arg != "self"
            ]
            fields += [
                Field(arg.arg, _ann(arg.annotation), default is not None)
                for arg, default in zip(a.kwonlyargs, a.kw_defaults)
            ]
            return fields
    return []


def _public_methods(cls: ast.ClassDef) -> list[ast.FunctionDef]:
    return [it for it in cls.body if isinstance(it, ast.FunctionDef) and not it.name.startswith("_")]


# ---------------------------------------------------------------- conjure ----
def _extract_conjure(root: Path) -> list[Element]:
    tree = ast.parse((root / "nominal_api" / "_impl.py").read_text(encoding="utf-8"))
    els: list[Element] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name.endswith("Visitor"):
            continue  # visitors mirror unions; the union row already captures the change
        m = _CONJURE_NAME.match(node.name)
        module, name = (m.group(1), m.group(2)) if m else ("", node.name)
        bases = _base_attrs(node)
        if "Service" in bases:
            for meth in _public_methods(node):
                params = [a.arg for a in meth.args.args if a.arg != "self"]
                els.append(Element("service", module, f"{name}.{meth.name}", params=params, returns=_ann(meth.returns)))
        elif "ConjureEnumType" in bases:
            members = sorted(
                t.id
                for it in node.body
                if isinstance(it, ast.Assign)
                for t in it.targets
                if isinstance(t, ast.Name) and t.id.isupper()
            )
            els.append(Element("enum", module, name, members=members))
        elif "ConjureUnionType" in bases:
            els.append(Element("union", module, name, fields=_init_fields(node)))
        elif "ConjureBeanType" in bases:
            els.append(Element("bean", module, name, fields=_init_fields(node)))
    return els


# ------------------------------------------------------------------ proto ----
def _proto_module(path: Path, protos_root: Path) -> str:
    rel = path.relative_to(protos_root).parent
    return ".".join(rel.parts) if rel.parts else "<root>"


def _is_message(cls: ast.ClassDef) -> bool:
    return "Message" in _base_attrs(cls)


def _is_enum(cls: ast.ClassDef) -> bool:
    if any(isinstance(b, ast.Name) and b.id == "int" for b in cls.bases):
        return True
    return any(k.arg == "metaclass" for k in cls.keywords)


def _enum_member_names(cls: ast.ClassDef) -> list[str]:
    return [
        it.target.id
        for it in cls.body
        if isinstance(it, ast.AnnAssign) and isinstance(it.target, ast.Name)
        and it.target.id.isupper() and it.target.id != "DESCRIPTOR"
    ]


def _proto_enum_members(cls: ast.ClassDef, classes: dict[str, ast.ClassDef]) -> list[str]:
    """Members across both stub styles: declared in-class (older) or held by a
    `_XxxEnumTypeWrapper` metaclass (newer mypy-protobuf).
    """
    found = _enum_member_names(cls)
    if found:
        return sorted(found)
    meta = next((k.value for k in cls.keywords if k.arg == "metaclass"), None)
    wname = meta.id if isinstance(meta, ast.Name) else meta.attr if isinstance(meta, ast.Attribute) else None
    wrapper = classes.get(wname) if wname else None
    return sorted(_enum_member_names(wrapper)) if wrapper else []


def _extract_proto(root: Path) -> list[Element]:
    protos_root = root / "nominal" / "protos"
    els: list[Element] = []
    for pyi in sorted(protos_root.rglob("*_pb2.pyi")):
        module = _proto_module(pyi, protos_root)
        tree = ast.parse(pyi.read_text(encoding="utf-8"))
        classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or node.name.startswith("_"):
                continue  # skip private helper classes (e.g. _XxxEnumTypeWrapper)
            if _is_enum(node):
                els.append(Element("enum", module, node.name, members=_proto_enum_members(node, classes)))
            elif _is_message(node):
                els.append(Element("message", module, node.name, fields=_init_fields(node)))
    for grpc in sorted(protos_root.rglob("*_pb2_grpc.py")):
        module = _proto_module(grpc, protos_root)
        for node in ast.parse(grpc.read_text(encoding="utf-8")).body:
            if isinstance(node, ast.ClassDef) and node.name.endswith("Servicer"):
                service = node.name[: -len("Servicer")]
                for meth in _public_methods(node):
                    els.append(Element("rpc", module, f"{service}.{meth.name}"))
    return els


def extract(transport: str, root: Path) -> list[Element]:
    return _extract_conjure(root) if transport == "conjure" else _extract_proto(root)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ("conjure", "proto"):
        sys.exit("usage: extract_surface.py {conjure|proto} <unpacked-wheel-root>")
    print(to_jsonl(extract(sys.argv[1], Path(sys.argv[2]))))
