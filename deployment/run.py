import requests
import json
import base64
import sys
from pathlib import Path
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import HTML

from deployment.config import Config
from anytree import Node, RenderTree
from helpers.fabric import get_lakehouse_id, create_lakehouse, get_lakehouse_id, delete_lakehouse, create_notebook, get_lakehouses
from helpers.general import compute_md5_hash


class Runner:
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
        self.items_tgt = self._get_items_tgt()
        self.items_git = self._get_items_git()
        

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
        self.run_source_checks()
        self.diff = self._get_diff()
        self.plan_is_current = True
        self.lakehouse_mapping_is_current = False

    def print_plan(self):
        if not self.plan_is_current:
            print("run plan first")
            return
        node_root = Node("Deployment")
        node_lh = Node("Lakehouses", parent=node_root)
        node_lh_new = Node("New", parent=node_lh)
        node_lh_ignore = Node("Ignore", parent=node_lh)
        node_lh_delete = Node("Delete", parent=node_lh)

        node_nb = Node("Notebooks", parent=node_root)
        node_nb_new = Node("New", parent=node_nb)
        node_nb_update = Node("Update - not supported yet", parent=node_nb)
        node_nb_delete = Node("Delete - not supported yet", parent=node_nb)


        for new_lh in self.diff['lakehouse']['new']:
            Node(new_lh, parent=node_lh_new)
        for ignore_lh in self.diff['lakehouse']['ignore']:
            Node(ignore_lh, parent=node_lh_ignore)
        for dangling_lh in self.diff['lakehouse']['dangling']:
            Node(dangling_lh, parent=node_lh_delete)

        for new_nb in self.diff['notebook']['new']:
            Node(new_nb, parent=node_nb_new)
        for update_nb in self.diff['notebook']['update']:
            Node(update_nb, parent=node_nb_update)
        for dangling_nb in self.diff['notebook']['dangling']:
            Node(dangling_nb, parent=node_nb_delete)

        print("\nHere is your deployment tree:\n")
        for pre, fill, node in RenderTree(node_root):
            print(f"{pre}{node.name}")

        print("")

    def run(self):
        print("")
        if not self.plan_is_current:
            print("run plan first")
            return
        
        # create new lakehouses
        for new_lh in self.diff['lakehouse']['new']:
            item_definition = self.get_lakehouse_git_definition(new_lh)
            print(f"...Creating Lakehouse {new_lh}") 
            create_lakehouse(self.config.user_headers, self.config.target_workspace_id, item_definition)

        # delete dangling lakehouses
        for del_lh in self.diff['lakehouse']['dangling']:
            print(f"...Deleting Lakehouse {del_lh}")
            id = get_lakehouse_id(self.config.user_headers, self.config.target_workspace_id, del_lh)
            delete_lakehouse(self.config.user_headers, self.config.target_workspace_id, id)

        # update the lakehouse mapping
        self._update_default_lakehouse_mapping()

        # create new notebooks
        for new_nb in self.diff['notebook']['new']:
            print(f"...Creating Notebook {new_nb}")
            self.create_notebook_from_local_repo(new_nb, self.config.repo_local_path / (new_nb + '.Notebook'))

        print("...All done.")
    
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
        print("\n...Running source checks.")
        default_lakehouse = next((obj for obj in self.items_git if obj.get("displayName") == "z_default_lakehouse" and obj.get("type") == "Lakehouse"), None)
        if not default_lakehouse:
            print("ERROR: no z_default_lakehouse in source")
            sys.exit(1)
        print("......TEST: source contains a z_default_lakehouse -> PASSED")
        
        # TODO: notebooks should only be connected to lakehouses in their own workspace.

        print("...All source checks passed.")

    def create_notebook_from_local_repo(self, display_name, folder_path: Path):
        if not self.lakehouse_mapping_is_current:
            print("ERROR: update the lakehouse mapping before creating notebooks")
            return 
        
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{self.config.target_workspace_id}/items'
        # read notebook-content file
        with open(folder_path / 'notebook-content.py', 'r') as file:
                nb_content_string = file.read()

        # switch workspace ids
        for ws_src in self.mapping.get('workspace').keys():
            ws_tgt = self.mapping.get('workspace').get(ws_src)
            print(f"......Changing workspace {ws_src} for {ws_tgt}")
            nb_content_string = nb_content_string.replace(ws_src, ws_tgt)

        # switch lakehouse ids
        for lh_src in self.mapping.get('lakehouse').keys():
            lh_tgt = self.mapping.get('lakehouse').get(lh_src)
            print(f"......Changing lakehouse {lh_src} for {lh_tgt}")
            nb_content_string = nb_content_string.replace(lh_src, lh_tgt)

        nb_content_bytes = nb_content_string.encode('utf-8')
        nb_content_b64 = base64.b64encode(nb_content_bytes).decode('utf-8')

        status_code = create_notebook(self.config.user_headers, self.config.target_workspace_id, display_name, nb_content_b64)
        
        return status_code
    
