"""Family name -> creator class."""
from creators.base import FamilyCreator
from creators.concox.creator import ConcoxCreator
from creators.queclink.creator import QueclinkCreator
from creators.ruptela.creator import RuptelaCreator
from creators.teltonika.creator import TeltonikaCreator
from creators.topflytech.creator import TopflytechCreator

CREATORS: dict[str, type[FamilyCreator]] = {
    "teltonika": TeltonikaCreator,
    "concox": ConcoxCreator,
    "topflytech": TopflytechCreator,
    "ruptela": RuptelaCreator,
    "queclink": QueclinkCreator,
}


def get_creator(family: str, device_tz: int = 7) -> FamilyCreator:
    """New creator instance (one per scenario x family, creators keep state like serial number)."""
    if family not in CREATORS:
        raise KeyError(f"family '{family}' belum punya creator")
    creator = CREATORS[family]()
    if hasattr(creator, "device_tz"):
        creator.device_tz = device_tz
    return creator
