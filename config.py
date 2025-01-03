from azure.identity import InteractiveBrowserCredential
import tomllib

from pathlib import Path

class Config:
    def __init__(self, use_cached_token: bool):
        with open("config/deploy.toml", "rb") as f:
            deploy_config = tomllib.load(f)
            self.target_workspace_id = deploy_config.get("target").get("workspace_id")
            self.source_workspace_id = deploy_config.get("source").get("workspace_id")
            self.repo_remote_url = deploy_config.get("repo_remote_url")
            self.az_tenant_id = deploy_config.get('az_tenant_id')

        if not use_cached_token:
            self.cache_access_token_interactive()

    def cache_access_token_interactive(self, path='temp/token.txt'):
        scope = ['https://analysis.windows.net/powerbi/api/.default']
        credential = InteractiveBrowserCredential(tenant_id=self.az_tenant_id)
        user_token = credential.get_token(*scope).token
        with open(path, "w") as f:
            f.write(user_token)
        self.token = user_token
        self.user_headers = {"Authorization": f"Bearer {user_token}"}

    def set_repo_local_path(self, path: Path):
        self.repo_local_path = path

    def set_user_headers(self, headers):
        self.user_headers = headers
    
    def set_user_token(self, token):
        self.user_token = token
