import pytest
from nominal.sdk import NominalClient, Run
from uuid import uuid4


def _update_run_attribute_and_revert(run: Run, attribute, value) -> None:
    initial_title = run.title
    initial_description = run.description
    initial_labels = run.labels
    initial_properties = run.properties
    initial_attribute = getattr(run, attribute)

    run.update(**{attribute: value})
    assert getattr(run, attribute) == value
    run.update(**{attribute: initial_attribute})
    assert getattr(run, attribute) == initial_attribute

    assert run.title == initial_title
    assert run.description == initial_description
    assert run.properties == initial_properties
    assert run.labels == initial_labels


@pytest.mark.e2e
def test_search_for_run(client: NominalClient, run: Run) -> None:
    runs = list(client.search_runs(start=run.start))
    assert len(runs) > 0
    assert any(r.rid == run.rid for r in runs)


@pytest.mark.e2e
def test_update_run(run: Run) -> None:
    assert run.rid != ""
    # update each attribute individually
    _update_run_attribute_and_revert(run, "title", run.title + str(uuid4()))
    _update_run_attribute_and_revert(run, "description", run.description + str(uuid4()))
    _update_run_attribute_and_revert(run, "labels", (str(uuid4()),))
    _update_run_attribute_and_revert(run, "properties", {str(uuid4()): str(uuid4())})
