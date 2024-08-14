import requests
from requests.adapters import CaseInsensitiveDict
from typing import Type, TypeVar
from conjure_python_client import ServiceConfiguration

T = TypeVar("T")
def create_service(service_class: Type[T], uri: str) -> T:
    config = ServiceConfiguration()

    session = requests.Session()
    session.headers = CaseInsensitiveDict({"User-Agent": "nominal-python"})

    return service_class(
        session,
        [uri],
        config.connect_timeout,
        config.read_timeout,
        None,
        False
    )
