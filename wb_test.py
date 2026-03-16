"""Test script for the Nominal multiplayer workbook session."""

import time

from pycrdt import Array
import json
from nominal.core import EventType, NominalClient
from nominal.experimental.multiplayer import WorkbookSession


def main():
    nom_client = NominalClient.from_profile("production")
    workbook_rid = "ri.scout.cerulean-staging.notebook.04e5dffe-9c9b-40f1-b133-abf7f41a2e3b"

    workbook = nom_client.get_workbook(workbook_rid)
    print(workbook.nominal_url)

    with WorkbookSession.create(workbook) as session:
        session.on_update(lambda state: print("update:", state["workbook"]["metadata"]["title"]))
        state = session.get_state()
        print("title:", state["workbook"]["metadata"]["title"])
        
        print(json.dumps(state))
        # Keep the connection open to observe updates from other clients.
        time.sleep(60)


if __name__ == "__main__":
    main()
