from azure.identity import InteractiveBrowserCredential
import requests
import tomllib
import json
from pathlib import Path

class Config:
    def __init__(self):
        with open("config/auth.toml", "rb") as f:
            auth_config = tomllib.load(f)
            self.az_tenant_id = auth_config.get('az_tenant_id')

        with open("config/deploy.toml", "rb") as f:
            deploy_config = tomllib.load(f)
            self.target_workspace_id = deploy_config.get("target").get("workspace_id")
            self.repo_local_path = deploy_config.get("repo_local_path")

        scope = ['https://analysis.windows.net/powerbi/api/.default']

        credential = InteractiveBrowserCredential(tenant_id=self.az_tenant_id)
        user_token = credential.get_token(*scope).token

        self.user_headers = {"Authorization": f"Bearer {user_token}"}

class Workspace:
    def __init__(self, config: Config):
        self.config = config
        self.items_tgt = self._get_items_tgt()
        self.items_git = self._get_items_git()
        
    def _get_items_tgt(self):
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{self.config.target_workspace_id}/items'
        r = requests.get(url, headers=self.config.user_headers)
        return json.loads(r.text).get("value")
    
    def _get_items_git(self):
        base_directory = Path(self.config.repo_local_path)

        items = []
        # lakehouses
        lakehouse_folders = [folder for folder in base_directory.rglob("*") if folder.is_dir() and folder.name.endswith(".Lakehouse")]    

        for folder in lakehouse_folders:
            with open(folder / ".platform") as f:
                data = json.load(f).get('metadata')
                items.append(data)
        return items
    
    def get_diff(self):
        diff = {"lakehouse": {"new": None, "ignore": None, "dangling": None}}

        # lakehouse
        lh_names_git = [item.get('displayName') for item in self.items_git if item.get("type") == "Lakehouse"]
        lh_names_tgt = [item.get('displayName') for item in self.items_tgt if item.get("type") == "Lakehouse"]
    
        new_lh_names = list(set(lh_names_git).difference(lh_names_tgt))
        ignore_lh_names = list(set(lh_names_git).intersection(lh_names_tgt))
        dangling_lh_names = list(set(lh_names_tgt).difference(lh_names_git))

        diff['lakehouse']['new'] = new_lh_names
        diff['lakehouse']['ignore'] = ignore_lh_names
        diff['lakehouse']['dangling'] = dangling_lh_names

        return diff



if __name__ == "__main__":

    config = Config()
    ws = Workspace(config)

    diff = ws.get_diff()

    print(diff)








