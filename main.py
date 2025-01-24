from prompt_toolkit import prompt
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import HTML
from prompt_toolkit.shortcuts import yes_no_dialog
from prompt_toolkit.completion import WordCompleter
from pathlib import Path
import tempfile
import os
import sys
from helpers.general import clear_terminal

from deployment.config import Config
from deployment.run import Runner

class App:
    def __init__(self):
        self._handle_repo_settings()

    def _handle_repo_settings(self):
        options = ["local", "public"]
        options_completer = WordCompleter(options, ignore_case=True)

        # Prompt the user to choose between "local" or "custom"
        choice = prompt(
            "Use local (default=temp/repo/fabric-workspace) or public repo? (Type 'local' or 'public'): ",
            completer=options_completer
        ).strip().lower()

        if choice == "local":
            path = prompt("Enter relative path (Enter for default path): ").strip()
            if not path:
                path = "temp/repo/fabric-workspace"
            self.use_local_repo = True
            self.local_repo_path = path
        elif choice == "public":
            # Prompt the user to enter a custom repository URL
            public_repo = prompt("Please enter the custom repository URL: ").strip()
            self.use_local_repo = False
            self.public_repo_url = public_repo 
        else:
            print("Invalid choice.")
            sys.exit(1)

    def _run_or_exit(self, prompt_str):
        options = ["yes", "no"]
        options_completer = WordCompleter(options, ignore_case=True)

        # Prompt the user to choose between "yes" or "no"
        choice = prompt(
            prompt_str,
            completer=options_completer
        ).strip().lower()

        if choice == "yes":
            pass
        elif choice == "no":
            print("Exiting...")
            sys.exit(0)
        else:
            print("Invalid choice.")
            sys.exit(1)

def deploy_with_repo(config, app, repo_path, is_temp=False):
    """
    Handles the deployment process with the given repository path.
    
    Args:
        config: The configuration object for deployment.
        app: The application object handling user interaction.
        repo_path: Path to the repository to use for deployment.
        is_temp: Whether the repository path is temporary and needs cleanup.
    """
    config.set_repo_local_path(Path(repo_path))
    if is_temp:
        print(f"Cloning repository to temporary directory: {repo_path}")
        os.system(f"git clone {config.repo_remote_url} {repo_path}")
    else:
        print(f"...Using local repo {repo_path}")

    dep = Runner(config)
    app._run_or_exit("Run preview deployment? (Type 'yes' or 'no'): ")
    dep.compute_plan()
    dep.print_plan()
    app._run_or_exit("Run deployment? (Type 'yes' or 'no'): ")
    dep.run()

def main():
    clear_terminal()
    app = App()
    config = Config()

    if app.use_local_repo:
        local_repo_path = "temp/repo/fabric-workspace"
        deploy_with_repo(config, app, local_repo_path)
    else: 
        with tempfile.TemporaryDirectory() as temp_dir:
            deploy_with_repo(config, app, temp_dir, is_temp=True)

if __name__ == '__main__':
    main()