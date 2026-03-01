from __future__ import annotations

from pathlib import Path

from matey.templates import (
    CIProvider,
    TemplateFile,
    parse_target_list,
    render_ci_template,
    render_config_template,
)


def render_ci(*, provider: CIProvider, targets: str | None) -> TemplateFile:
    parsed_targets = parse_target_list(targets)
    return render_ci_template(provider, targets=parsed_targets)


def render_config(*, targets: str | None) -> str:
    parsed_targets = parse_target_list(targets)
    return render_config_template(targets=parsed_targets)


def write_template_file(*, rendered: TemplateFile, force: bool) -> Path:
    if rendered.path.exists() and not force:
        raise FileExistsError(str(rendered.path))
    rendered.path.parent.mkdir(parents=True, exist_ok=True)
    rendered.path.write_text(rendered.content, encoding="utf-8")
    return rendered.path
