import os
import tempfile
from pathlib import Path

from config import Config
from deployment import Workspace


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








