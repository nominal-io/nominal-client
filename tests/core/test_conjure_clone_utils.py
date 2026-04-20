"""Tests for conjure_clone_utils: UUID extraction/replacement and RID override logic."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.id_utils.id_utils import UUID_PATTERN
from nominal.experimental.migration.utils.conjure_clone_utils import (
    _generate_uuid_mapping,
    _replace_uuids_in_obj,
    clone_conjure_objects_with_rid_overrides,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_A = "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa"
_B = "bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb"
_C = "cccccccc-3333-3333-3333-cccccccccccc"

_ASSET_RID = "ri.scout.cerulean-staging.asset.dddddddd-4444-4444-4444-dddddddddddd"
_NEW_ASSET_RID = "ri.scout.prod.asset.eeeeeeee-5555-5555-5555-eeeeeeeeeeee"
_RUN_RID = "ri.scout.cerulean-staging.run.ffffffff-6666-6666-6666-ffffffffffff"
_NEW_RUN_RID = "ri.scout.prod.run.11111111-7777-7777-7777-111111111111"


# ---------------------------------------------------------------------------
# _generate_uuid_mapping and _replace_uuids_in_obj
# ---------------------------------------------------------------------------


class TestUuidMappingAndReplacement:
    """Tests the dict-level helpers with plain JSON-like dicts."""

    def test_uuid_key_values_extracted_replaced_and_non_uuid_strings_untouched(self) -> None:
        """UUID_KEY values are extracted into the mapping and substituted; other strings are left alone.
        Also verifies that prefixed UUIDs (like RIDs) preserve their prefix when regenerated.
        """
        prefixed_rid = f"ri.scout.cerulean-staging.notebook.{_A}"
        obj = {"id": _A, "functionUuid": _B, "label": "my-chart", "rid": prefixed_rid}

        mapping = _generate_uuid_mapping([obj])

        # Both UUID_KEY values were extracted
        assert _A in mapping
        assert _B in mapping
        assert prefixed_rid in mapping
        # They were given distinct fresh values
        assert mapping[_A] != _A
        assert mapping[_B] != _B
        assert mapping[prefixed_rid] != prefixed_rid
        # The notebook RID preserves its prefix
        assert mapping[prefixed_rid].startswith("ri.scout.cerulean-staging.notebook.")

        result = _replace_uuids_in_obj(obj, mapping)
        assert result["id"] == mapping[_A]
        assert result["functionUuid"] == mapping[_B]
        assert result["rid"] == mapping[prefixed_rid]
        assert result["label"] == "my-chart"  # plain string untouched

    def test_asset_rid_not_auto_extracted_but_replaced_via_override(self) -> None:
        """Asset/run RIDs under non-UUID_KEY keys are not extracted automatically,
        but are replaced when explicitly added to the mapping as overrides.
        Also confirms override takes precedence if the same string somehow appeared in auto-mapping.
        """
        obj = {
            "id": _A,
            "variable": _ASSET_RID,  # non-UUID_KEY key → not auto-extracted
            "assetRidVariableName": _RUN_RID,  # non-UUID_KEY key → not auto-extracted
        }

        mapping = _generate_uuid_mapping([obj])
        assert _ASSET_RID not in mapping
        assert _RUN_RID not in mapping

        # Add overrides (simulating what clone_conjure_objects_with_rid_overrides does)
        mapping.pop(_ASSET_RID, None)
        mapping.pop(_RUN_RID, None)
        mapping[_ASSET_RID] = _NEW_ASSET_RID
        mapping[_RUN_RID] = _NEW_RUN_RID

        result = _replace_uuids_in_obj(obj, mapping)
        assert result["variable"] == _NEW_ASSET_RID
        assert result["assetRidVariableName"] == _NEW_RUN_RID
        assert result["id"] == mapping[_A]  # internal UUID still regenerated
        assert result["id"] != _A

    def test_cross_object_uuid_coherence(self) -> None:
        """The same UUID appearing across multiple objects gets the same replacement in both.
        This covers the layout↔content panel-ID coherence requirement.
        """
        obj1 = {"id": _A}
        obj2 = {"id": _A, "functionUuid": _B}

        mapping = _generate_uuid_mapping([obj1, obj2])
        # _A is only added to the mapping once — same new value used for both objects
        result1 = _replace_uuids_in_obj(obj1, mapping)
        result2 = _replace_uuids_in_obj(obj2, mapping)

        assert result1["id"] == result2["id"]
        assert result1["id"] != _A

    def test_nested_json_encoded_string_values_are_also_replaced(self) -> None:
        """Values that are JSON-encoded strings are unpacked and have their contents replaced too."""
        import json

        inner = {"variable": _ASSET_RID}
        obj = {"state": json.dumps(inner)}  # JSON-encoded string field

        mapping = {_ASSET_RID: _NEW_ASSET_RID}
        result = _replace_uuids_in_obj(obj, mapping)

        reparsed = json.loads(result["state"])
        assert reparsed["variable"] == _NEW_ASSET_RID


# ---------------------------------------------------------------------------
# clone_conjure_objects_with_rid_overrides (integration via patched Conjure serde)
# ---------------------------------------------------------------------------


class TestCloneWithRidOverrides:
    """End-to-end tests via the public function, with Conjure encode/decode patched to be identity ops."""

    def _make_source_obj(self, d: dict) -> MagicMock:
        obj = MagicMock()
        obj.__class__ = MagicMock  # satisfies type() call in the function
        return obj

    @patch("nominal.experimental.migration.utils.conjure_clone_utils.ConjureDecoder")
    @patch("nominal.experimental.migration.utils.conjure_clone_utils.ConjureEncoder")
    def test_overrides_applied_and_internal_uuids_regenerated(
        self, mock_encoder: MagicMock, mock_decoder: MagicMock
    ) -> None:
        """RID overrides are substituted; UUID_KEY values are regenerated; empty overrides also work."""
        input_dict = {"id": _A, "variable": _ASSET_RID}
        mock_encoder.do_encode.return_value = input_dict
        mock_decoder.return_value.do_decode.side_effect = lambda obj, t: obj

        src = MagicMock()
        clone_conjure_objects_with_rid_overrides([src], rid_overrides={_ASSET_RID: _NEW_ASSET_RID})

        result_dict = mock_decoder.return_value.do_decode.call_args[0][0]
        assert result_dict["variable"] == _NEW_ASSET_RID  # override applied
        assert result_dict["id"] != _A  # internal UUID regenerated
        assert UUID_PATTERN.match(result_dict["id"])  # still a valid UUID

        # Empty overrides: only UUID regeneration, no RID substitution
        mock_decoder.reset_mock()
        clone_conjure_objects_with_rid_overrides([src], rid_overrides={})
        result_dict_no_overrides = mock_decoder.return_value.do_decode.call_args[0][0]
        assert result_dict_no_overrides["variable"] == _ASSET_RID  # unchanged (not in mapping)
        assert result_dict_no_overrides["id"] != _A  # still regenerated

    @patch("nominal.experimental.migration.utils.conjure_clone_utils.ConjureDecoder")
    @patch("nominal.experimental.migration.utils.conjure_clone_utils.ConjureEncoder")
    def test_tuple_overload_preserves_types_and_shares_uuid_mapping(
        self, mock_encoder: MagicMock, mock_decoder: MagicMock
    ) -> None:
        """When called with a tuple of two objects, cross-object UUID coherence is maintained
        and the return type mirrors the input (tuple).
        """
        layout_dict = {"id": _A}  # panel UUID in layout
        content_dict = {"id": _A, "variable": _ASSET_RID}  # same panel UUID in content

        mock_encoder.do_encode.side_effect = [layout_dict, content_dict]
        mock_decoder.return_value.do_decode.side_effect = lambda obj, t: obj

        src1, src2 = MagicMock(), MagicMock()
        result = clone_conjure_objects_with_rid_overrides((src1, src2), rid_overrides={_ASSET_RID: _NEW_ASSET_RID})

        assert isinstance(result, tuple)
        calls = mock_decoder.return_value.do_decode.call_args_list
        result_layout = calls[0][0][0]
        result_content = calls[1][0][0]

        # Same shared UUID gets the same replacement in both objects
        assert result_layout["id"] == result_content["id"]
        assert result_layout["id"] != _A
        assert result_content["variable"] == _NEW_ASSET_RID
