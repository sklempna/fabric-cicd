from azure.identity import InteractiveBrowserCredential
import tomllib
import os
import jwt
import time
from pathlib import Path

token_path = "temp/token.txt"

class Config:
    def __init__(self):
        with open("config/deploy.toml", "rb") as f:
            deploy_config = tomllib.load(f)
            self.target_workspace_id = deploy_config.get("target").get("workspace_id")
            self.source_workspace_id = deploy_config.get("source").get("workspace_id")
            self.repo_remote_url = deploy_config.get("repo_remote_url")
            self.az_tenant_id = deploy_config.get('az_tenant_id')

        token = self._retrieve_token()
        self.token = token
        self.user_headers = {"Authorization": f"Bearer {token}"}

    def cache_access_token_interactive(self, path='temp/token.txt'):
        scope = ['https://analysis.windows.net/powerbi/api/.default']
        credential = InteractiveBrowserCredential(tenant_id=self.az_tenant_id)
        user_token = credential.get_token(*scope).token
        with open(path, "w") as f:
            f.write(user_token)
        # self.token = user_token
        # self.user_headers = {"Authorization": f"Bearer {user_token}"}

    def _retrieve_token(self): 
        """
        Check if there is an unexpired cached token and otherwise log user in.
        Store the token at token_path and return it
        """
        if not os.path.isfile(token_path):
            print(f"chaching token at {token_path}")
            self.cache_access_token_interactive(path=token_path)
        else:
            with open(token_path, "r") as f:
                cached_token = f.read()
            decoded = jwt.decode(cached_token, options={"verify_signature": False}) 
            expiration_ts = decoded.get("exp")
            current_ts = int(time.time())
            if current_ts > expiration_ts:
                print(f"Cached token expired. chaching token at {token_path}")
                self.cache_access_token_interactive(path=token_path)
            else:
                print(f"Using cached token at {token_path}")
        with open(token_path, "r") as f:
            token = f.read()
        return token
        
    def set_repo_local_path(self, path: Path):
        self.repo_local_path = path

    def set_user_headers(self, headers):
        self.user_headers = headers
    
    def set_user_token(self, token):
        self.user_token = token
