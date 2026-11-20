"""Runtime configuration.

The app can talk to a real cluster in three ways:

* ``in-cluster`` — the pod's service account (default when running in a pod).
* ``kubeconfig`` — a local kubeconfig file, for an operator workstation.
* ``host`` — an explicit API server URL plus bearer token, which is how the
  bundled simulator is wired up in docker compose.

Environment variables (all optional):

``DRAIN_KUBE_MODE``
    ``in-cluster`` | ``kubeconfig`` | ``host``.  Defaults to ``in-cluster``
    when a service account token exists, otherwise ``kubeconfig``.
``DRAIN_KUBE_HOST``
    API server URL for ``host`` mode.
``DRAIN_KUBE_TOKEN`` / ``DRAIN_KUBE_TOKEN_FILE``
    Bearer token for ``host`` mode.
``DRAIN_KUBE_INSECURE``
    ``1`` to skip TLS verification (simulator only).
``DRAIN_KUBE_CA_CERT``
    Path to a CA bundle for ``host`` mode.
``DRAIN_KUBECONFIG`` / ``DRAIN_KUBE_CONTEXT``
    For ``kubeconfig`` mode.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SERVICE_ACCOUNT_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")


@dataclass(frozen=True)
class Settings:
    mode: str
    host: str | None
    token: str | None
    insecure: bool
    ca_cert: str | None
    kubeconfig: str | None
    context: str | None
    title: str = "节点排空值班台"

    @classmethod
    def from_env(cls) -> "Settings":
        mode = os.environ.get("DRAIN_KUBE_MODE")
        if not mode:
            if os.environ.get("DRAIN_KUBE_HOST"):
                mode = "host"
            elif SERVICE_ACCOUNT_DIR.joinpath("token").exists():
                mode = "in-cluster"
            else:
                mode = "kubeconfig"
        token = os.environ.get("DRAIN_KUBE_TOKEN")
        token_file = os.environ.get("DRAIN_KUBE_TOKEN_FILE")
        if not token and token_file:
            path = Path(token_file)
            if path.exists():
                token = path.read_text().strip()
        return cls(
            mode=mode,
            host=os.environ.get("DRAIN_KUBE_HOST"),
            token=token,
            insecure=os.environ.get("DRAIN_KUBE_INSECURE", "") in ("1", "true", "yes"),
            ca_cert=os.environ.get("DRAIN_KUBE_CA_CERT"),
            kubeconfig=os.environ.get("DRAIN_KUBECONFIG") or os.environ.get("KUBECONFIG"),
            context=os.environ.get("DRAIN_KUBE_CONTEXT"),
            title=os.environ.get("DRAIN_TITLE", "节点排空值班台"),
        )


def _sa(name: str) -> str | None:
    path = SERVICE_ACCOUNT_DIR / name
    return str(path) if path.exists() else None


def build_client(settings: Settings):
    """Construct the Kubernetes client for the configured mode."""
    from .kube import KubeClient, KubeError

    if settings.mode == "host":
        if not settings.host:
            raise KubeError("DRAIN_KUBE_MODE=host 需要设置 DRAIN_KUBE_HOST")
        return KubeClient.from_host(
            settings.host,
            token=settings.token,
            verify_ssl=not settings.insecure,
            ca_cert=settings.ca_cert or _sa("ca.crt"),
        )
    if settings.mode == "kubeconfig":
        return KubeClient.from_kubeconfig(settings.kubeconfig, settings.context)
    if settings.mode == "in-cluster":
        return KubeClient.from_incluster(
            token=settings.token, ca_cert=settings.ca_cert or _sa("ca.crt")
        )
    raise KubeError(f"未知的 DRAIN_KUBE_MODE: {settings.mode}")
