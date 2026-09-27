import httpx
import pytest

from papollo import ApolloError
from papollo._core import Settings, normalize_namespace, parse_config_response, sign


def make_settings(**overrides) -> Settings:
    values = dict(
        server_url="http://apollo:8080",
        app_id="demo",
        cluster="default",
        secret=None,
        ip=None,
        label=None,
        timeout=5.0,
    )
    values.update(overrides)
    return Settings(**values)


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
