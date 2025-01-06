from prompt_toolkit import prompt
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import HTML
from prompt_toolkit.shortcuts import yes_no_dialog
from prompt_toolkit.completion import WordCompleter
from pathlib import Path
import tempfile
import os
import sys
import helpers

from config import Config
from deployment import Deployment

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


def main():
    helpers.clear_terminal()
    app = App()
    config = Config()

    if app.use_local_repo:
        config.set_repo_local_path(Path("temp/repo/fabric-workspace"))
        print("...Using local repo temp/repo/fabric-workspace")
        dep = Deployment(config)
    else: 
        with tempfile.TemporaryDirectory() as temp_dir:
            config.set_repo_local_path(Path(temp_dir))
            print(f"Cloning repository to temporary directory: {temp_dir}")
            command = f"git clone {config.repo_remote_url} {temp_dir}"
            os.system(command)
            dep = Deployment(config)
    
    app._run_or_exit("Run preview deployment? (Type 'yes' or 'no'): ")

    dep.compute_plan()
    dep.print_plan()

    app._run_or_exit("Run deployment? (Type 'yes' or 'no'): ")

    dep.run()


    


if __name__ == '__main__':
    main()