def retrieve_asset(
    client: nominal.NominalClient, platform: str, serial_num: str
) -> nominal.Asset:
    properties = {"platform": platform, "serial_num": serial_num}
    existing_assets = client.search_assets(properties=properties)
    if len(existing_assets) > 1:
        raise RuntimeError(
            f"Too many assets ({len(existing_assets)}) found with properties {properties}"
        )
    elif len(existing_assets) == 0:
        raise RuntimeError(f"No such asset found with properties {properties}")
    else:
        return existing_assets[0]
