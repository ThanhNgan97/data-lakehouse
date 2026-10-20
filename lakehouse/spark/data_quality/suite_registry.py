"""Dataset-to-suite routing without embedding business rules in the runner."""

from __future__ import annotations

from collections.abc import Iterable


class SuiteRegistry:
    """In-memory registry used by orchestration and dataset adapters."""

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], tuple[str, ...]] = {}

    def register(
        self,
        dataset_id: str,
        data_layer: str,
        suite_names: Iterable[str],
    ) -> None:
        key = (dataset_id.strip(), data_layer.strip().lower())
        suites = tuple(dict.fromkeys(name.strip() for name in suite_names))
        if not all(key) or not suites or any(not name for name in suites):
            raise ValueError("dataset, layer, and suite names must be non-empty")
        self._routes[key] = suites

    def resolve(self, dataset_id: str, data_layer: str) -> tuple[str, ...]:
        return self._routes.get(
            (dataset_id.strip(), data_layer.strip().lower()),
            (),
        )

    def is_registered(self, dataset_id: str, data_layer: str) -> bool:
        return bool(self.resolve(dataset_id, data_layer))


SUITE_REGISTRY = SuiteRegistry()
