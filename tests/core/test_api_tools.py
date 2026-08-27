from __future__ import annotations

from nominal.core._utils.api_tools import create_links, create_proto_links

EVERY_SPELLING = ["https://a", ("https://b", "B"), {"url": "https://c", "title": "C"}, {"url": "https://d"}]


def test_create_proto_links_accepts_every_link_spelling() -> None:
    """A link may be a url, a (url, title) tuple, or a dict with an optional title."""
    links = create_proto_links(EVERY_SPELLING)

    assert [(link.url, link.title) for link in links] == [
        ("https://a", ""),
        ("https://b", "B"),
        ("https://c", "C"),
        ("https://d", ""),
    ]


def test_create_links_agrees_with_the_proto_builder() -> None:
    """Both builders share one dispatch; only the absent title differs, since proto has no null string."""
    links = create_links(EVERY_SPELLING)

    assert [(link.url, link.title) for link in links] == [
        ("https://a", None),
        ("https://b", "B"),
        ("https://c", "C"),
        ("https://d", None),
    ]
