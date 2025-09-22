from nominal import NominalClient
from nominal.experimental.compute import module
from nominal.experimental.compute.dsl import params
from nominal.experimental.compute.dsl.exprs import NumericExpr
from nominal.experimental.compute.module._functions import get_module


@module.defn
def new_frame_transforms2(asset: params.StringVariable) -> module.ModuleVariables:
    """Frame-transform utilities for quaternions (channels x, y, z, w)."""
    return {
        "w": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q1"),
        "x": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q2"),
        "y": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q3"),
        "z": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q4"),
    }


@new_frame_transforms2.func
def roll(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute roll (rotation around X) from quaternion (x, y, z, w)."""
    return (w * x + y * z).scale(2).atan2((x * x + y * y).scale(-2).offset(1))


@new_frame_transforms2.func
def pitch(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute pitch (rotation around Y) from quaternion (x, y, z, w)."""
    return (w * y - z * x).scale(2).asin()


@new_frame_transforms2.func
def yaw(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute yaw (rotation around Z) from quaternion (x, y, z, w)."""
    return (w * z + x * y).scale(2).atan2((y * y + z * z).scale(-2).offset(1))


client = NominalClient.from_profile("orion-staging")
orion_asset = client.get_asset("ri.scout.gov-staging.asset.435c0bf0-4cc7-446a-8333-11a85c0bdea3")
# mod = get_module(client, "ri.scout.gov-staging.module.19450549-1ade-49ee-8b12-69bac5d311e2")
# print(mod)
# mod = mod.update(new_frame_transforms2)
# print(mod)
mod = new_frame_transforms2.register(client)
print(mod)
app = mod.apply(asset=orion_asset.rid)
print(app)
