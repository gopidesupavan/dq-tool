#!/usr/bin/env python3
"""Extract Python API metadata from qualink source files using the ast module.

Outputs a JSON file (docs/src/_data/api.json) that Eleventy consumes at build
time to render API reference pages automatically — no manual copying of
class/method signatures into markdown.

Usage:
    python docs/scripts/extract_api.py

The output is organised by module group matching the API doc pages:
    core, checks, constraints, comparison, formatters, config
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any

# Paths -----------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent  # qualink/
SRC_ROOT = REPO_ROOT / "src" / "qualink"
OUT_FILE = REPO_ROOT / "docs" / "src" / "_data" / "api.json"

# Module group → directory name mapping
GROUPS: dict[str, str] = {
    "core": "core",
    "checks": "checks",
    "constraints": "constraints",
    "comparison": "comparison",
    "formatters": "formatters",
    "config": "config",
}

# Skip these files
SKIP_FILES = {"__init__.py"}

# Skip private names (but keep __init__, __str__, __repr__)
KEEP_DUNDERS = {"__init__", "__str__", "__repr__", "__post_init__"}


# --- AST helpers -------------------------------------------------------------

def _unparse(node: ast.expr | None) -> str:
    """Convert an AST node to its source-code string."""
    if node is None:
        return ""
    try:
        result = ast.unparse(node)
        # Clean up common repr patterns for readability
        if result == "''":
            result = '""'
        return result
    except Exception:
        return ""


def _get_docstring(node: ast.AST) -> str:
    """Extract the docstring from a class/function/module node."""
    return ast.get_docstring(node) or ""


def _is_private(name: str) -> bool:
    """Return True for names we should skip."""
    if name in KEEP_DUNDERS:
        return False
    return name.startswith("_")


def _get_decorators(node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """Return decorator strings (e.g. ['dataclass(frozen=True)', 'staticmethod'])."""
    result = []
    for dec in node.decorator_list:
        result.append(ast.unparse(dec))
    return result


def _extract_arg(arg: ast.arg) -> dict[str, str]:
    """Extract name and type annotation from a function argument."""
    return {
        "name": arg.arg,
        "type": _unparse(arg.annotation),
    }


def _format_param(p: dict[str, str]) -> str:
    """Format a single parameter for a signature string."""
    parts = [p["name"]]
    if p.get("type"):
        parts.append(f': {p["type"]}')
    if p.get("default") and not p["name"].startswith("*"):
        parts.append(f' = {p["default"]}')
    return "".join(parts)


def _format_signature(name: str, params: list[dict], returns: str,
                      is_async: bool = False, decorators: list[str] | None = None) -> str:
    """Build a full method/function signature string."""
    parts = []
    if decorators:
        for d in ("staticmethod", "classmethod", "abstractmethod"):
            if d in decorators:
                parts.append(f"@{d} ")
    if is_async:
        parts.append("async ")
    parts.append(f"{name}(")
    parts.append(", ".join(_format_param(p) for p in params))
    parts.append(")")
    if returns:
        parts.append(f" → {returns}")
    return "".join(parts)


def _extract_function(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, Any]:
    """Extract metadata from a function or method node."""
    args = node.args

    # Build parameters list (skip 'self' and 'cls')
    params = []
    all_args = args.args[:]

    # Determine number of positional-only and normal args
    num_args = len(all_args)
    num_defaults = len(args.defaults)
    default_offset = num_args - num_defaults

    for i, arg in enumerate(all_args):
        if arg.arg in ("self", "cls"):
            continue
        info = _extract_arg(arg)
        # Check for default value
        default_idx = i - default_offset
        if default_idx >= 0 and default_idx < len(args.defaults):
            info["default"] = _unparse(args.defaults[default_idx])
        params.append(info)

    # Keyword-only args
    kw_defaults = args.kw_defaults
    for i, arg in enumerate(args.kwonlyargs):
        if arg.arg in ("self", "cls"):
            continue
        info = _extract_arg(arg)
        if i < len(kw_defaults) and kw_defaults[i] is not None:
            info["default"] = _unparse(kw_defaults[i])
        params.append(info)

    # *args
    if args.vararg:
        params.append({
            "name": f"*{args.vararg.arg}",
            "type": _unparse(args.vararg.annotation),
        })

    # **kwargs
    if args.kwarg:
        params.append({
            "name": f"**{args.kwarg.arg}",
            "type": _unparse(args.kwarg.annotation),
        })

    decorators = _get_decorators(node)
    returns = _unparse(node.returns)
    is_async = isinstance(node, ast.AsyncFunctionDef)
    sig = _format_signature(node.name, params, returns, is_async, decorators)

    return {
        "name": node.name,
        "is_async": is_async,
        "decorators": decorators,
        "params": params,
        "returns": returns,
        "docstring": _get_docstring(node),
        "signature": sig,
    }


def _extract_enum_members(node: ast.ClassDef) -> list[dict[str, str]]:
    """Extract enum member assignments from a class body."""
    members = []
    for stmt in node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and not target.id.startswith("_"):
                    members.append({
                        "name": target.id,
                        "value": _unparse(stmt.value),
                    })
        elif isinstance(stmt, ast.AnnAssign) and stmt.target and isinstance(stmt.target, ast.Name):
            name = stmt.target.id
            if not name.startswith("_"):
                members.append({
                    "name": name,
                    "value": _unparse(stmt.value) if stmt.value else "",
                })
    return members


def _extract_dataclass_fields(node: ast.ClassDef) -> list[dict[str, str]]:
    """Extract field definitions from a dataclass body."""
    fields = []
    for stmt in node.body:
        if isinstance(stmt, ast.AnnAssign) and stmt.target and isinstance(stmt.target, ast.Name):
            name = stmt.target.id
            field_info: dict[str, str] = {
                "name": name,
                "type": _unparse(stmt.annotation),
            }
            if stmt.value is not None:
                val_str = _unparse(stmt.value)
                # Detect field(default_factory=...) patterns
                field_info["default"] = val_str
            fields.append(field_info)
    return fields


def _is_enum(node: ast.ClassDef) -> bool:
    """Check if any base class looks like an Enum."""
    for base in node.bases:
        name = _unparse(base)
        if "Enum" in name or "IntEnum" in name:
            return True
    return False


def _is_dataclass(node: ast.ClassDef) -> bool:
    """Check if the class has a @dataclass decorator."""
    for dec in node.decorator_list:
        dec_str = ast.unparse(dec)
        if "dataclass" in dec_str:
            return True
    return False


def _extract_class(node: ast.ClassDef) -> dict[str, Any]:
    """Extract full metadata from a class definition."""
    bases = [_unparse(b) for b in node.bases]
    decorators = _get_decorators(node)

    is_enum = _is_enum(node)
    is_dc = _is_dataclass(node)

    # Fields (dataclass) or enum members
    fields: list[dict[str, str]] = []
    enum_members: list[dict[str, str]] = []
    if is_dc:
        fields = _extract_dataclass_fields(node)
    if is_enum:
        enum_members = _extract_enum_members(node)

    # Methods and properties
    methods = []
    properties = []
    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_private(item.name):
                if item.name not in KEEP_DUNDERS:
                    continue
            info = _extract_function(item)
            # Separate properties from methods
            if any(d in ("property", "functools.cached_property") for d in info["decorators"]):
                properties.append(info)
            else:
                methods.append(info)

    # Build class header signature
    dec_prefix = " ".join(f"@{d}" for d in decorators) + " " if decorators else ""
    base_suffix = f"({', '.join(bases)})" if bases else ""
    class_signature = f"{dec_prefix}class {node.name}{base_suffix}"
    class_name_only = f"{node.name}{base_suffix}"

    # Build init signature
    init_signature = ""
    for m in methods:
        if m["name"] == "__init__" and m["params"]:
            init_signature = f'{node.name}({", ".join(_format_param(p) for p in m["params"])})'
            break

    return {
        "name": node.name,
        "bases": bases,
        "decorators": decorators,
        "docstring": _get_docstring(node),
        "is_enum": is_enum,
        "is_dataclass": is_dc,
        "fields": fields,
        "enum_members": enum_members,
        "methods": methods,
        "properties": properties,
        "class_signature": class_signature,
        "class_name_only": class_name_only,
        "init_signature": init_signature,
    }


def _extract_module_functions(tree: ast.Module) -> list[dict[str, Any]]:
    """Extract top-level functions from a module."""
    funcs = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_private(node.name):
                continue
            funcs.append(_extract_function(node))
    return funcs


# --- Main extraction ---------------------------------------------------------

def extract_file(filepath: Path, module_path: str) -> dict[str, Any]:
    """Parse a single Python file and extract all public API metadata."""
    source = filepath.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(filepath))

    classes = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            if _is_private(node.name):
                continue
            classes.append(_extract_class(node))

    functions = _extract_module_functions(tree)

    return {
        "module": module_path,
        "filename": filepath.name,
        "docstring": _get_docstring(tree),
        "classes": classes,
        "functions": functions,
    }


def extract_group(group_name: str, dir_name: str) -> dict[str, Any]:
    """Extract all modules in a group directory."""
    group_dir = SRC_ROOT / dir_name
    if not group_dir.is_dir():
        return {"modules": []}

    modules = []
    for py_file in sorted(group_dir.glob("*.py")):
        if py_file.name in SKIP_FILES:
            continue
        module_path = f"qualink.{dir_name}.{py_file.stem}"
        try:
            mod = extract_file(py_file, module_path)
            # Only include if it has public content
            if mod["classes"] or mod["functions"]:
                modules.append(mod)
        except SyntaxError as e:
            print(f"WARNING: Syntax error in {py_file}: {e}", file=sys.stderr)

    return {"modules": modules}


def main() -> None:
    data: dict[str, Any] = {}
    for group_name, dir_name in GROUPS.items():
        data[group_name] = extract_group(group_name, dir_name)

    # Ensure output directory exists
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    # Print summary
    total_classes = 0
    total_methods = 0
    total_functions = 0
    for group_name, group_data in data.items():
        for mod in group_data["modules"]:
            for cls in mod["classes"]:
                total_classes += 1
                total_methods += len(cls["methods"]) + len(cls["properties"])
            total_functions += len(mod["functions"])

    print(f"✓ Extracted {total_classes} classes, {total_methods} methods/properties, "
          f"{total_functions} functions → {OUT_FILE.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()

