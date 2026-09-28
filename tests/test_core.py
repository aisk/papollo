from dataclasses import replace
from typing import Any

import httpx
import pytest

from papollo import ApolloError
from papollo._core import (
    Notification,
    Settings,
    normalize_namespace,
    parse_config_response,
    parse_notifications_response,
    sign,
)


def make_settings(**overrides: Any) -> Settings:
    defaults = Settings("http://apollo:8080", "demo", "default", None, None, None, 5.0)
    return replace(defaults, **overrides)


def test_sign_matches_java_client() -> None:
    # Test vector from apollo-java SignatureTest.
    assert (
        sign(
            "1576478257344",
            "/configs/100004458/default/application?ip=10.0.0.1",
            "df23df3f59884980844ff3dada30fa97",
        )
        == "EoKyziXvKqzHgwx+ijDJwgVTDgE="
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("application", "application"),
        ("application.properties", "application"),
        ("Application.PROPERTIES", "Application"),
        ("app.json", "app.json"),
        ("app.yaml", "app.yaml"),
    ],
)
def test_normalize_namespace(name: str, expected: str) -> None:
    assert normalize_namespace(name) == expected


def test_request_omits_empty_params() -> None:
    with httpx.Client() as http:
        request = make_settings().build_config_request(http, "application", None)
    assert str(request.url) == "http://apollo:8080/configs/demo/default/application"
    assert "Authorization" not in request.headers


def test_request_params_and_escaping() -> None:
    settings = make_settings(server_url="http://gw/apollo", ip="10.0.0.1", label="gray")
    with httpx.Client() as http:
        request = settings.build_config_request(http, "a/b", "r1")
    assert (
        request.url.raw_path
        == b"/apollo/configs/demo/default/a%2Fb?releaseKey=r1&ip=10.0.0.1&label=gray"
    )


def test_request_signed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("time.time", lambda: 1576478257.344)
    settings = make_settings(
        secret="df23df3f59884980844ff3dada30fa97", ip="10.0.0.1", app_id="100004458"
    )
    with httpx.Client() as http:
        request = settings.build_config_request(http, "application", None)
    assert request.headers["Timestamp"] == "1576478257344"
    assert request.headers["Authorization"] == "Apollo 100004458:EoKyziXvKqzHgwx+ijDJwgVTDgE="


def test_request_messages() -> None:
    with httpx.Client() as http:
        request = make_settings().build_config_request(
            http, "application", "r1", {"demo+default+application": 7}
        )
    assert request.url.params["messages"] == '{"details":{"demo+default+application":7}}'


def test_notifications_request() -> None:
    settings = make_settings(ip="10.0.0.1")
    with httpx.Client() as http:
        request = settings.build_notifications_request(http, {"application": -1, "app.json": 3})
    assert request.url.path == "/notifications/v2"
    assert dict(request.url.params) == {
        "appId": "demo",
        "cluster": "default",
        "notifications": '[{"namespaceName":"application","notificationId":-1},'
        '{"namespaceName":"app.json","notificationId":3}]',
        "ip": "10.0.0.1",
    }
    # The server holds the request for 60 seconds.
    assert request.extensions["timeout"] == {
        "connect": 5.0,
        "read": 90.0,
        "write": 5.0,
        "pool": 5.0,
    }


def test_notifications_request_signed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("time.time", lambda: 1576478257.344)
    settings = make_settings(secret="df23df3f59884980844ff3dada30fa97")
    with httpx.Client() as http:
        request = settings.build_notifications_request(http, {"application": -1})
    path = request.url.raw_path.decode()
    assert request.headers["Timestamp"] == "1576478257344"
    assert request.headers["Authorization"] == (
        f"Apollo demo:{sign('1576478257344', path, 'df23df3f59884980844ff3dada30fa97')}"
    )


def test_parse_ok() -> None:
    response = httpx.Response(200, json={"configurations": {"k": "v"}, "releaseKey": "r1"})
    snapshot = parse_config_response(response, "application")
    assert snapshot is not None
    assert snapshot.release_key == "r1"
    assert dict(snapshot.configurations) == {"k": "v"}
    with pytest.raises(TypeError):
        snapshot.configurations["k"] = "x"  # type: ignore[index]


def test_parse_not_modified() -> None:
    assert parse_config_response(httpx.Response(304), "application") is None


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(404),
        httpx.Response(401),
        httpx.Response(500),
        httpx.Response(200, content=b"not json"),
        httpx.Response(200, json={"releaseKey": "r1"}),
    ],
)
def test_parse_errors(response: httpx.Response) -> None:
    with pytest.raises(ApolloError) as info:
        parse_config_response(response, "application")
    assert info.value.status_code == response.status_code


def test_parse_notifications() -> None:
    body = [
        {
            "namespaceName": "application",
            "notificationId": 12,
            "messages": {"details": {"demo+default+application": 12}},
        },
        {"namespaceName": "app.json", "notificationId": 3, "messages": None},
        {"namespaceName": "other", "notificationId": 4},
    ]
    assert parse_notifications_response(httpx.Response(200, json=body)) == [
        Notification("application", 12, {"demo+default+application": 12}),
        Notification("app.json", 3, {}),
        Notification("other", 4, {}),
    ]


def test_parse_notifications_not_modified() -> None:
    assert parse_notifications_response(httpx.Response(304)) == []


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(401),
        httpx.Response(500),
        httpx.Response(200, content=b"not json"),
        httpx.Response(200, json={"namespaceName": "application"}),
        httpx.Response(200, json=[{"namespaceName": "application"}]),
        httpx.Response(200, json=[{"namespaceName": "a", "notificationId": 1, "messages": 1}]),
    ],
)
def test_parse_notifications_errors(response: httpx.Response) -> None:
    with pytest.raises(ApolloError) as info:
        parse_notifications_response(response)
    assert info.value.status_code == response.status_code
