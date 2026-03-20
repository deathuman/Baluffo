from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from parsel import Selector


def _normalize_tag_names(name: Any) -> list[str]:
    if name is None:
        return []
    if isinstance(name, str):
        return [name]
    if isinstance(name, Iterable):
        return [str(value) for value in name if value]
    return [str(name)]


def _build_name_xpath(name: Any) -> str:
    names = _normalize_tag_names(name)
    if not names:
        return "*"
    if len(names) == 1:
        return names[0]
    return "*[" + " or ".join(f"self::{tag}" for tag in names) + "]"


class SoupNode:
    def __init__(self, selector: Selector) -> None:
        self._selector = selector

    def __bool__(self) -> bool:
        return bool(self._selector.get())

    @property
    def attrs(self) -> dict[str, str]:
        return dict(self._selector.attrib)

    def get(self, key: str, default: Any = None) -> Any:
        return self.attrs.get(key, default)

    def get_text(self, separator: str = "", strip: bool = False) -> str:
        texts = [text.get() for text in self._selector.xpath(".//text()")]
        if strip:
            texts = [text.strip() for text in texts if text and text.strip()]
        return separator.join(texts)

    @property
    def stripped_strings(self):
        parts = re.split(r"[\r\n]+", self.get_text("\n", strip=True))
        return (part.strip() for part in parts if part and part.strip())

    def select(self, query: str) -> list["SoupNode"]:
        return [SoupNode(node) for node in self._selector.css(query)]

    def find(self, name: Any = None, attrs: dict[str, Any] | None = None, **kwargs: Any) -> "SoupNode | None":
        xpath = f".//{_build_name_xpath(name)}"
        predicates: list[str] = []
        for attr_name, attr_value in (attrs or {}).items():
            predicates.append(f'@{attr_name}="{attr_value}"')
        for attr_name, attr_value in kwargs.items():
            if attr_value is True:
                predicates.append(f"@{attr_name}")
            elif attr_value not in (None, False):
                predicates.append(f'@{attr_name}="{attr_value}"')
        if predicates:
            xpath += "[" + " and ".join(predicates) + "]"
        match = self._selector.xpath(xpath)
        first = match[0] if match else None
        return SoupNode(first) if first is not None else None

    def find_parent(self, name: Any = None) -> "SoupNode | None":
        tag_expr = _build_name_xpath(name)
        xpath = f"ancestor::{tag_expr}[1]" if tag_expr != "*" else "ancestor::*[1]"
        match = self._selector.xpath(xpath)
        first = match[0] if match else None
        return SoupNode(first) if first is not None else None


class HtmlSoup(SoupNode):
    def __init__(self, html: str, _parser: str = "html.parser") -> None:
        super().__init__(Selector(text=html or ""))
