from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence, overload

from nominal_api import api, security_api_workspace
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._datasource_types import DataSourceType
from nominal.core._utils.api_tools import HasRid, RefreshableMixin


@dataclass(frozen=True)
class Workspace(HasRid, RefreshableMixin[security_api_workspace.Workspace]):
    rid: str
    """RID of this workspace."""

    id: str
    """Internal unique identifier for the workspace within the organization.
    The ID must be lower case alphanumetric characters, optionally separated by hyphens.
    """

    org: str
    """RID of the Organization backing the workspace."""

    display_name: str | None
    """User-facing display name set by organization admins for this Workspace.
    This is the name that users will see when selecting workspaces within the product.
    """

    _clients: _Clients = field(repr=False)
    _workspace_settings: security_api_workspace.WorkspaceSettings = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def workspace(self) -> security_api_workspace.WorkspaceService: ...

    def _get_latest_api(self) -> security_api_workspace.Workspace:
        """Retrieve the latest state of this Workspace from the API."""
        return self._clients.workspace.get_workspace(self._clients.auth_header, self.rid)

    @property
    def preferred_refnames(self) -> Mapping[str, DataSourceType]:
        """Get mapping of preferred refnames to their respective datasource types that they apply towards."""
        refname_settings = self._workspace_settings.ref_names
        if refname_settings is None:
            return {}

        v1_preferred_refnames = refname_settings.v1
        if v1_preferred_refnames is None:
            return {}

        return {
            refname_and_type.name: DataSourceType._from_conjure(refname_and_type.type)
            for refname_and_type in v1_preferred_refnames
        }

    @property
    def preferred_procedure_rids(self) -> Sequence[str]:
        """Get the list of preferred procedure rids.

        Unlike other procedures which can only be kicked off from the procedures page, preferred procedures
        may be kicked off from the homepage of the application
        """
        procedure_settings = self._workspace_settings.procedures
        if procedure_settings is None:
            return []

        v1_preferred_procedures = procedure_settings.v1
        if v1_preferred_procedures is None:
            return []

        return v1_preferred_procedures.workspace_procedures

    @overload
    def update(self, *, display_name: str | None = None) -> Self: ...
    @overload
    def update(
        self,
        *,
        display_name: str | None = None,
        preferred_procedures: Sequence[str],
        preferred_refnames: Mapping[str, DataSourceType],
    ) -> Self: ...
    def update(
        self,
        *,
        display_name: str | None = None,
        preferred_procedures: Sequence[str] | None = None,
        preferred_refnames: Mapping[str, DataSourceType] | None = None,
    ) -> Self:
        """Replace workspace metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Args:
            display_name: User-visible display name for the workspace within the application when switching workspaces.
            preferred_procedures: List of preferred procedures to expose as runnable on the frontend.
            preferred_refnames: Mapping of preferred refnames to the types of datasource they may be used for.

        Returns:
            Updated workspace metadata


        NOTE: This replaces the metadata rather than appending it. To append to preferred refnames,
            merge them before calling this method, e.g.:

            ```
            workspace.update(preferred_refnames={**workspace.preferred_refnames, "apples": DataSourceType.DATASET})
            ```

        NOTE: If setting preferred procedures or preferred refnames, *both* must be set, or *neither* must be set.
              Otherwise, setting one but not the other would clear out configuration for the other.
        """
        display_name_req = None
        if display_name is not None:
            display_name_req = security_api_workspace.UpdateOrRemoveWorkspaceDisplayName(display_name=display_name)

        workspace_settings_req = None
        if (preferred_procedures is not None and preferred_refnames is None) or (
            preferred_procedures is None and preferred_refnames is not None
        ):
            raise ValueError(
                "Both preferred_procedures and preferred_refnames must be provided together, or neither should be "
                "provided. This prevents accidentally clearing one setting when updating the other."
            )
        elif preferred_procedures is not None and preferred_refnames is not None:
            workspace_settings_req = security_api_workspace.WorkspaceSettings(
                procedures=security_api_workspace.ProcedureSettings(
                    v1=security_api_workspace.ProcedureSettingsV1(workspace_procedures=[*preferred_procedures])
                ),
                ref_names=security_api_workspace.PreferredRefNameConfiguration(
                    v1=[
                        api.RefNameAndType(name=refname, type=datasource_type._to_conjure())
                        for refname, datasource_type in preferred_refnames.items()
                    ]
                ),
            )

        req = security_api_workspace.UpdateWorkspaceRequest(
            display_name=display_name_req,
            settings=workspace_settings_req,
        )
        resp = self._clients.workspace.update_workspace(self._clients.auth_header, req, self.rid)
        return self._refresh_from_api(resp)

    @classmethod
    def _from_conjure(cls, clients: _Clients, workspace: security_api_workspace.Workspace) -> Self:
        return cls(
            rid=workspace.rid,
            id=workspace.id,
            org=workspace.org,
            display_name=workspace.display_name,
            _clients=clients,
            _workspace_settings=workspace.settings,
        )
