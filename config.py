from azure.identity import InteractiveBrowserCredential
import tomllib
import os
import jwt
import time
from pathlib import Path
import sys
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import HTML

TOKEN_CACHE_PATH = "temp/token.txt"
CONFIG_FILE_PATH = "config/deploy.toml"

class Config:
    def __init__(self):
        self._process_config_file(CONFIG_FILE_PATH)
        token = self._retrieve_token()
        self.token = token
        self.user_headers = {"Authorization": f"Bearer {token}"}

    def _process_config_file(self, path):
        with open(path, "rb") as f:
            deploy_config = tomllib.load(f)
            self.target_workspace_id = deploy_config.get("target").get("workspace_id")
            self.source_workspace_id = deploy_config.get("source").get("workspace_id")
            self.repo_remote_url = deploy_config.get("repo_remote_url")
            self.az_tenant_id = deploy_config.get('az_tenant_id')

        for value in ['target.workspace_id', 'source.workspace_id', 'repo_remote_url', 'az_tenant_id']:
            if not eval('self.' + value.replace('.','_')):
                print(HTML(f"<ansired><b>ERROR</b>: property {value} missing from config file at {path}</ansired>"))
                sys.exit(1)

    def cache_access_token_interactive(self):
        scope = ['https://analysis.windows.net/powerbi/api/.default']
        credential = InteractiveBrowserCredential(tenant_id=self.az_tenant_id)
        user_token = credential.get_token(*scope).token
        with open(TOKEN_CACHE_PATH, "w") as f:
            f.write(user_token)

    def _retrieve_token(self): 
        """
        Check if there is an unexpired cached token and otherwise log user in.
        Store the token at token_path and return it
        """
        if not os.path.isfile(TOKEN_CACHE_PATH):
            print(f"caching token at {TOKEN_CACHE_PATH}")
            self.cache_access_token_interactive()
        else:
            with open(TOKEN_CACHE_PATH, "r") as f:
                cached_token = f.read()
            decoded = jwt.decode(cached_token, options={"verify_signature": False}) 
            expiration_ts = decoded.get("exp")
            current_ts = int(time.time())
            if current_ts > expiration_ts:
                print(f"...Cached token expired. chaching token at {TOKEN_CACHE_PATH}")
                self.cache_access_token_interactive()
            else:
                print(f"...Using cached token at {TOKEN_CACHE_PATH}")
        with open(TOKEN_CACHE_PATH, "r") as f:
            token = f.read()
        return token
        
    def set_repo_local_path(self, path: Path):
        self.repo_local_path = path

    def set_user_headers(self, headers):
        self.user_headers = headers
    
    def set_user_token(self, token):
        self.user_token = token
