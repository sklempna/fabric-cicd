from azure.identity import InteractiveBrowserCredential
import requests
import tomllib
import json
import base64
import sys
import os
import tempfile
import hashlib
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


class Workspace:
    def __init__(self, config: Config):
        """
        Connects to the target workspace and obtains the git and workspace items.
        """
        self.config = config
        self.plan = None
        self.plan_is_current = False
        self.items_tgt = None
        self.items_git = None
        self.diff = None
        self.mapping = {}
        self.mapping['workspace'] = {self.config.source_workspace_id : self.config.target_workspace_id}
        self.mapping['lakehouse'] = {}
        self.lakehouse_mapping_is_current = False

    def _update_default_lakehouse_mapping(self):
        """
        Gets the mapping of z_default_lakehouse ids between the source and the 
        target workspace, to be substituted in notebook definitions.

        TODO: do this for other lakehouses than just the z_default_lakehouse
        """
        src_id = get_lakehouse_id(self.config.user_headers, self.config.source_workspace_id, 'z_default_lakehouse')
        tgt_id = get_lakehouse_id(self.config.user_headers, self.config.target_workspace_id, 'z_default_lakehouse')
        self.mapping['lakehouse'] = {src_id : tgt_id}
        self.lakehouse_mapping_is_current = True
        
    def _get_items_tgt(self):
        """
        Get a list of items from the target workspace
        """
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{self.config.target_workspace_id}/items'
        r = requests.get(url, headers=self.config.user_headers)
        return json.loads(r.text).get("value")
    
    def _get_items_git(self):
        """
        Get a list of items from the git repo.
        """
        base_directory = self.config.repo_local_path

        items = []
        # lakehouses
        lakehouse_folders = [folder for folder in base_directory.rglob("*") if folder.is_dir() and folder.name.endswith(".Lakehouse")]    

        for folder in lakehouse_folders:
            with open(folder / ".platform") as f:
                data = json.load(f).get('metadata')
                items.append(data)
        
    
        notebook_folders = [folder for folder in base_directory.rglob("*") if folder.is_dir() and folder.name.endswith(".Notebook")]  
        for folder in notebook_folders:
            notebook_hash = compute_md5_hash(folder / "notebook-content.py")
            with open(folder / ".platform") as f:
                data = json.load(f).get('metadata')
                data["hash"] = notebook_hash
                items.append(data)
            
        return items
    
    def get_lakehouse_git_definition(self, display_name):
        # TODO: Error here if notebook path is invalid
        lh = next((item for item in self.items_git if item['type'] == 'Lakehouse' and item['displayName'] == display_name), None)
        if not lh:
            print(f"Lakehouse {display_name} not found in repository")
            return
        return lh
    
    def compute_plan(self):
        self.items_tgt = self._get_items_tgt()
        self.items_git = self._get_items_git()
        self.run_source_checks()
        self.diff = self._get_diff()
        self.plan_is_current = True
        self.lakehouse_mapping_is_current = False

    def print_plan(self):
        if not self.plan_is_current:
            print("run plan first")
            return

        for new_lh in self.diff['lakehouse']['new']:
            print(f"create lakehouse {new_lh}") 
        for ignore_lh in self.diff['lakehouse']['ignore']:
            print(f"ignore lakehouse {ignore_lh}") 
        for dangling_lh in self.diff['lakehouse']['dangling']:
            print(f"delete lakehouse {dangling_lh}") 

        for new_nb in self.diff['notebook']['new']:
            print(f"create notebook {new_nb}") 
        for update_nb in self.diff['notebook']['update']:
            print(f"update notebook {update_nb} -- not supported yet") 
        for dangling_nb in self.diff['notebook']['dangling']:
            print(f"delete notebook {dangling_nb} -- not supported yet") 

    def run(self):
        if not self.plan_is_current:
            print("run plan first")
            return
        
        # create new lakehouses
        for new_lh in self.diff['lakehouse']['new']:
            item_definition = self.get_lakehouse_git_definition(new_lh)
            print(f"creating lakehouse {new_lh}") 
            create_lakehouse(self.config.user_headers, self.config.target_workspace_id, item_definition)

        # delete dangling lakehouses
        for del_lh in self.diff['lakehouse']['dangling']:
            print(f"deleting lakehouse {del_lh}")
            id = get_lakehouse_id(self.config.user_headers, self.config.target_workspace_id, del_lh)
            delete_lakehouse(self.config.user_headers, self.config.target_workspace_id, id)

        # update the lakehouse mapping
        self._update_default_lakehouse_mapping()

        # create new notebooks
        for new_nb in self.diff['notebook']['new']:
            print(f"creating notebook {new_nb}")
            self.create_notebook_from_local_repo(new_nb, self.config.repo_local_path / (new_nb + '.Notebook'))


    
    def _get_diff(self):
        """
        Get a dict of the differences of the git repo and the target workspace.
        """
        diff = {
            "lakehouse": {"new": None, "ignore": None, "dangling": None},
            "notebook": {"new": None, "update": None, "dangling": None}
        }

        # lakehouse
        lh_names_git = [item.get('displayName') for item in self.items_git if item.get("type") == "Lakehouse"]
        lh_names_tgt = [item.get('displayName') for item in self.items_tgt if item.get("type") == "Lakehouse"]
    
        new_lh_names = list(set(lh_names_git).difference(lh_names_tgt))
        ignore_lh_names = list(set(lh_names_git).intersection(lh_names_tgt))
        dangling_lh_names = list(set(lh_names_tgt).difference(lh_names_git))

        diff['lakehouse']['new'] = new_lh_names
        diff['lakehouse']['ignore'] = ignore_lh_names
        diff['lakehouse']['dangling'] = dangling_lh_names

        # notebooks
        nb_names_git = [item.get('displayName') for item in self.items_git if item.get("type") == "Notebook"]
        nb_names_tgt = [item.get('displayName') for item in self.items_tgt if item.get("type") == "Notebook"]

        new_nb_names = list(set(nb_names_git).difference(nb_names_tgt))
        update_nb_names = list(set(nb_names_git).intersection(nb_names_tgt))
        dangling_nb_names = list(set(nb_names_tgt).difference(nb_names_git))

        diff['notebook']['new'] = new_nb_names
        diff['notebook']['update'] = update_nb_names
        diff['notebook']['dangling'] = dangling_nb_names

        return diff
    
    def run_source_checks(self):
        print("Running source checks.")
        default_lakehouse = next((obj for obj in self.items_git if obj.get("displayName") == "z_default_lakehouse" and obj.get("type") == "Lakehouse"), None)
        if not default_lakehouse:
            print("ERROR: no z_default_lakehouse in source")
            sys.exit(1)
        print("TEST: source contains a z_default_lakehouse -> PASSED")
        
        # TODO: notebooks should only be connected to lakehouses in their own workspace.

        print("All source checks passed.")

    def create_notebook_from_local_repo(self, display_name, folder_path: Path):
        if not self.lakehouse_mapping_is_current:
            print("ERROR: update the lakehouse mapping before createing notebooks")
            return 
        
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{self.config.target_workspace_id}/items'
        # read notebook-content file
        with open(folder_path / 'notebook-content.py', 'r') as file:
                nb_content_string = file.read()

        # switch workspace ids
        for ws_src in self.mapping.get('workspace').keys():
            ws_tgt = self.mapping.get('workspace').get(ws_src)
            print(f"changing workspace {ws_src} for {ws_tgt}")
            nb_content_string = nb_content_string.replace(ws_src, ws_tgt)

        # switch lakehouse ids
        for lh_src in self.mapping.get('lakehouse').keys():
            lh_tgt = self.mapping.get('lakehouse').get(lh_src)
            print(f"changing lakehouse {lh_src} for {lh_tgt}")
            nb_content_string = nb_content_string.replace(lh_src, lh_tgt)

        nb_content_bytes = nb_content_string.encode('utf-8')
        nb_content_b64 = base64.b64encode(nb_content_bytes).decode('utf-8')

        # encoded_nb_content = base64_encode_file(folder_path / 'notebook-content.py')
        payload = {
            "displayName": display_name,
            "type": "Notebook",
            "definition": {
                "format": "fabricGitSource",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": nb_content_b64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        # print(payload)
        r = requests.post(url, headers=self.config.user_headers, json=payload)
        return r.status_code

# helpers 

def compute_md5_hash(file_path):
    """
    Compute the MD5 hash of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The MD5 hash as a hexadecimal string.
    """
    try:
        # Read the entire file in binary mode and compute the hash
        with open(file_path, 'rb') as file:
            file_data = file.read()
            return hashlib.md5(file_data).hexdigest()
    except FileNotFoundError:
        return f"Error: File '{file_path}' not found."
    
def base64_encode_file(file_path):
    """
    Base64 encode the contents of a file.

    Args:
        file_path (str): The path to the file to encode.

    Returns:
        str: The Base64-encoded contents of the file, or an error message if the file cannot be read.
    """
    try:
        # Open the file in binary mode and read its contents
        with open(file_path, 'rb') as file:
            file_data = file.read()
        
        # Base64 encode the binary data
        encoded_data = base64.b64encode(file_data)
        
        # Convert the encoded bytes to a string and return
        return encoded_data.decode('utf-8')
    
    except FileNotFoundError:
        return f"Error: File '{file_path}' not found."
    except Exception as e:
        return f"Error: {e}"

    
# Fabric api wrapper

def create_lakehouse(auth_header, workspace_id, item_definition):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses'
    r = requests.post(url, headers=auth_header, data=item_definition)
    return json.loads(r.text).get("value")


def get_lakehouse_id(auth_header, workspace_id, display_name):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses'
    r = requests.get(url, headers=auth_header)
    values = json.loads(r.text).get("value")
    lh = next((item for item in values if item['type'] == 'Lakehouse' and item['displayName'] == display_name), None)
    if not lh:
        print(f'No lakehouse with display_name {display_name} found in workspace {workspace_id}')
        return
    return lh['id']

def delete_lakehouse(auth_header, workspace_id, lakehouse_id):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}'
    r = requests.delete(url, headers=auth_header)
    return 

if __name__ == "__main__":
    use_local = True 

    config = Config(use_local)

    if use_local:
        config.set_repo_local_path(Path("temp/repo/fabric-dbt-workspace"))
        print("using local repo temp/repo/fabric-dbt-workspace")

        # TODO: remove magic path
        with open("temp/token.txt", "r") as f: 
            # TODO: check for existence and validity of token
            user_token = f.read()
            config.set_user_token(user_token)
            config.set_user_headers({"Authorization": f"Bearer {user_token}"})
            ws = Workspace(config)

    else: 
        with tempfile.TemporaryDirectory() as temp_dir:
            config.set_repo_local_path(Path(temp_dir))
            print(f"Cloning repository to temporary directory: {temp_dir}")
            command = f"git clone {config.repo_remote_url} {temp_dir}"
            os.system(command)
            ws = Workspace(config)

        
    ws.compute_plan()
    print(ws.diff)








