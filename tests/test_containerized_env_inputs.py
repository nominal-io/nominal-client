from __future__ import annotations

from typing import Dict, List

import pytest

from nominal.experimental.containerized import containerized_env_inputs


def test_containerized_env_inputs_injects_files_secrets_and_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("__nominal_file_b", "/tmp/input_b.bin")
    monkeypatch.setenv("__nominal_file_a", "/tmp/input_a.bin")
    monkeypatch.setenv("__nominal_secret_auth", "secret-auth-token")
    monkeypatch.setenv("__nominal_parameter_mode", "production")
    monkeypatch.setenv("__nominal_parameter_retries", "3")
    monkeypatch.setenv("UNRELATED_ENV_VAR", "ignored")

    @containerized_env_inputs
    def wrapped(
        *,
        files: list[str],
        secrets: dict[str, str],
        parameters: dict[str, str],
    ) -> tuple[list[str], dict[str, str], dict[str, str]]:
        return files, secrets, parameters

    files, secrets, parameters = wrapped()
    assert files == ["/tmp/input_a.bin", "/tmp/input_b.bin"]
    assert secrets == {"auth": "secret-auth-token"}
    assert parameters == {"mode": "production", "retries": "3"}


def test_containerized_env_inputs_supports_typing_list_and_dict_annotations() -> None:
    @containerized_env_inputs
    def wrapped(*, files: List[str], secrets: Dict[str, str], parameters: Dict[str, str]) -> bool:
        return isinstance(files, list) and isinstance(secrets, dict) and isinstance(parameters, dict)

    assert wrapped()


def test_containerized_env_inputs_requires_files_parameter() -> None:
    with pytest.raises(TypeError, match="missing required parameter 'files'"):

        @containerized_env_inputs
        def wrapped(*, secrets: list[str], parameters: dict[str, str]) -> None:
            return


def test_containerized_env_inputs_requires_dict_annotation_for_secrets() -> None:
    with pytest.raises(TypeError, match="parameter 'secrets' must be annotated as dict"):

        @containerized_env_inputs
        def wrapped(*, files: list[str], secrets: list[str], parameters: dict[str, str]) -> None:
            return
