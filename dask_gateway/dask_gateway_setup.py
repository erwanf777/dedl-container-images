import base64
import uuid
import jwt
import aiohttp
import json
import os

from aiohttp import web
from traitlets import Dict, Unicode, default
from traitlets.config import LoggingConfigurable

from dask_gateway_server.auth import Authenticator, unauthorized, User

dask_roles = {
    "stack-dask-high": {
        "worker_cores": 12,
        "worker_memory": 24,
    },
    "stack-dask-medium": {
        "worker_cores": 4,
        "worker_memory": 8,
    },
    "stack-dask-low": {
        "worker_cores": 1,
        "worker_memory": 2,
    },
    "stack-dask-tuw": {
        "worker_cores": 2,
        "worker_memory": 2,
        "worker_cores_limit": 2,
        "worker_memory_limit": 2,
        "scheduler_cores": 2,
        "scheduler_memory": 2,
        "scheduler_cores_limit": 2,
        "scheduler_memory_limit": 2,
        "max_workers": 2,
    },
}


class JWTAuthenticator(Authenticator, LoggingConfigurable):
    jwks_urls = Dict(
        help="""
        A mapping of validator names to their JWKS URLs to get the public keys to verify JWT.
        The validator name is expected to be present in the "auth_extra" claim of the JWT.
        """,
        config=True,
    )

    @default("jwks_urls")
    def _default_jwks_urls(self):
        config: dict = json.loads(base64.b64decode(os.getenv("JWKS_URL", "")))
        jwks_urls = {}

        for auth_extra, jwks_url in config.items():
            jwks_urls[auth_extra] = jwks_url

        return jwks_urls

    async def setup(self, app):
        self.session = aiohttp.ClientSession()

    async def cleanup(self):
        if hasattr(self, "session"):
            await self.session.close()

    async def authenticate(self, request):
        self.log.info("Authenticating request...")
        auth_header: str = request.headers.get("Authorization")
        if not auth_header:
            self.log.info("No Authorization header.")
            raise web.HTTPUnauthorized(reason="No JWT in Headers.")

        try:
            assert auth_header.startswith("Bearer ")
        except ValueError:
            self.log.info("No 'Bearer' in Authorization header.")
            raise web.HTTPUnauthorized(reason="No 'Bearer' in Authorization header.")

        token = auth_header.split(" ")[-1]

        try:
            split_token = token.split("/")
        except Exception:
            self.log.error("Error splitting token.")
            raise unauthorized("jwt")

        if len(split_token) != 2:
            auth_extra = "eodc"
        else:
            auth_extra = split_token[0]
            token = split_token[1]

        if auth_extra not in self.jwks_urls:
            self.log.error(f"Unknown auth_extra '{auth_extra}'")
            raise unauthorized("jwt")
        else:
            self.jwks_url = self.jwks_urls[auth_extra]
            self._jwks_client = jwt.PyJWKClient(self.jwks_url)

        self.log.info(f"Authenticating with auth_extra '{auth_extra}'")
        if auth_extra == "eodc":
            return await eodc_validate_token(token, self._jwks_client)
        elif auth_extra == "tuw":
            return await tuw_validate_token(token, self._jwks_client)

        raise unauthorized("jwt")


async def pre_validation(token, jwks_client: jwt.PyJWKClient):
    signing_key = jwks_client.get_signing_key_from_jwt(token)
    try:
        data = jwt.decode(token, signing_key.key, algorithms=["RS256"])
    except jwt.exceptions.ExpiredSignatureError as e:
        raise unauthorized("Couldn't validate jwt")

    return data


async def eodc_validate_token(token, jwks_client: jwt.PyJWKClient):
    data = await pre_validation(token, jwks_client)

    if data and ("realm_access" in data) and ("roles" in data["realm_access"]):
        if "preferred_username" not in data:
            user_name = str(uuid.uuid4())
        else:
            user_name = data["preferred_username"]
        roles = set(data["realm_access"]["roles"])
        user_dask_role = sorted(roles.intersection(dask_roles))
        if len(user_dask_role) == 0:
            raise unauthorized("jwt")
        else:
            return User(
                user_name,
                groups=[user_dask_role[0]],
                admin=False,
            )

    raise unauthorized("Not authorized for Dask Gateway")


async def tuw_validate_token(token, jwks_client: jwt.PyJWKClient):
    data = await pre_validation(token, jwks_client)

    if ("eodc_access" in data) and (data["eodc_access"]):
        if "preferred_username" not in data:
            user_name = str(uuid.uuid4())
        else:
            user_name = "tuw_" + data["preferred_username"]
        return User(
            user_name,
            groups=["stack-dask-tuw"],
            admin=False,
        )

    raise unauthorized("Not authorized for Dask Gateway")


import uuid
from dask_gateway_server.backends.kubernetes import KubeBackend


class ExtendedKubeBackend(KubeBackend):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.log.info("[ExtendedKubeBackend] Initialized custom backend")

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)

        labels = {
            "eodc.dask/username": user.name,
            "eodc.dask/group": list(user.groups)[0],
        }
        config.scheduler_extra_pod_labels = labels
        config.worker_extra_pod_labels = labels

        obj = self.make_cluster_object(user.name, options, config)
        name = obj["metadata"]["name"]
        cluster_name = f"{config.namespace}.{name}"

        self.log.info(
            "[ExtendedKubeBackend] Creating labelled cluster %s for user %s",
            cluster_name,
            user.name,
        )

        await self.custom_client.create_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, config.namespace, "daskclusters", obj
        )
        return cluster_name
