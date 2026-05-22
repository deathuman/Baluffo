import json

import pytest

from src.jobs.adapters import provider_api
from src.jobs.adapters import provider_structured_listing as provider_structured_listing_runner
from tests.test_provider_migration import _bind_fake_deps, _FakeDeps


def test_bamboohr_provider_uses_careers_list_when_html_listing_has_no_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = _FakeDeps({})
    deps.set_registry_entries(
        "bamboohr",
        [
            {
                "name": "Streamline Studios (BambooHR)",
                "studio": "Streamline Studios",
                "adapter": "bamboohr",
                "listing_url": "https://streamlinestudios.bamboohr.com/careers",
                "enabledByDefault": True,
            }
        ],
    )
    _bind_fake_deps(monkeypatch, deps)

    payload = {
        "meta": {"totalCount": 1},
        "result": [
            {
                "id": "106",
                "jobOpeningName": "3D Character Artist",
                "employmentStatusLabel": "Freelancer",
                "departmentLabel": "Art",
                "location": {
                    "city": "Kuala Lumpur",
                    "country": "Malaysia",
                },
            }
        ],
    }

    class _JsonResponse:
        def __enter__(self) -> "_JsonResponse":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(payload).encode("utf-8")

    def fake_urlopen(request: object, timeout: int) -> _JsonResponse:
        assert timeout == 5
        assert request.full_url == "https://streamlinestudios.bamboohr.com/careers/list"
        return _JsonResponse()

    def fake_fetch(url: str, _timeout: int) -> str:
        assert url == "https://streamlinestudios.bamboohr.com/careers"
        return "<html><main id='root'></main></html>"

    monkeypatch.setattr(provider_structured_listing_runner, "urlopen", fake_urlopen)

    rows = provider_api.run_bamboohr_sources_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )

    assert rows == [
        {
            "sourceJobId": "bamboohr:106",
            "title": "3D Character Artist",
            "company": "Streamline Studios",
            "city": "Kuala Lumpur",
            "country": "Malaysia",
            "workType": "",
            "contractType": "Freelancer",
            "jobLink": "https://streamlinestudios.bamboohr.com/careers/106",
            "sector": "Game",
            "postedAt": "",
            "locations": ["Kuala Lumpur, Malaysia"],
            "locationSummary": "Kuala Lumpur, Malaysia",
            "department": "Art",
            "adapter": "bamboohr",
            "studio": "Streamline Studios",
            "source": "Streamline Studios (BambooHR)",
        }
    ]
    details = deps.SOURCE_DIAGNOSTICS["bamboohr_sources"]["details"][0]
    assert details["fetchedCount"] == 1
    assert details["keptCount"] == 1


def test_workday_provider_uses_cxs_api_when_html_listing_has_no_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps = _FakeDeps({})
    deps.set_registry_entries(
        "workday",
        [
            {
                "name": "Example Studio (Workday)",
                "studio": "Example Studio",
                "adapter": "workday",
                "listing_url": "https://example.wd5.myworkdayjobs.com/en-US/Company_Careers?q=game",
                "enabledByDefault": True,
            }
        ],
    )
    _bind_fake_deps(monkeypatch, deps)
    requests: list[tuple[str, dict[str, object]]] = []

    class _Response:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = json.dumps(payload).encode("utf-8")

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return self._payload

    def fake_urlopen(request: object, timeout: int) -> _Response:
        _ = timeout
        requests.append((request.full_url, json.loads(request.data.decode("utf-8"))))
        return _Response(
            {
                "total": 1,
                "jobPostings": [
                    {
                        "title": "Gameplay Programmer",
                        "externalPath": "/job/Remote/Gameplay-Programmer_JR100",
                        "locationsText": "Remote, United States",
                        "postedOn": "Posted 2 Days Ago",
                        "timeType": "Full time",
                    }
                ],
            }
        )

    monkeypatch.setattr(provider_structured_listing_runner, "urlopen", fake_urlopen)

    rows = provider_api.run_workday_sources_source(
        fetch_text=lambda _url, _timeout: "<html><main id='root'></main></html>",
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )

    assert len(rows) == 1
    assert requests == [
        (
            "https://example.wd5.myworkdayjobs.com/wday/cxs/example/Company_Careers/jobs",
            {
                "appliedFacets": {},
                "limit": 20,
                "offset": 0,
                "searchText": "game",
            },
        )
    ]
    assert rows[0]["title"] == "Gameplay Programmer"
    assert rows[0]["adapter"] == "workday"
    assert rows[0]["studio"] == "Example Studio"
    assert rows[0]["jobLink"] == (
        "https://example.wd5.myworkdayjobs.com/en-US/Company_Careers"
        "/job/Remote/Gameplay-Programmer_JR100"
    )
    assert deps.SOURCE_DIAGNOSTICS["workday_sources"]["details"][0]["fetchedCount"] == 1
