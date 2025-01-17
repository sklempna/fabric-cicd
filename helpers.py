from azure.identity import InteractiveBrowserCredential
import requests
import json
import base64
import hashlib
import os
import platform
import sys
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import HTML

# helpers 

def clear_terminal():
    # Check the operating system and run the appropriate clear command
    if platform.system() == "Windows":
        os.system("cls")  # Windows command to clear the terminal
    else:
        os.system("clear")  # Unix-based systems (Linux/Mac) command to clear the terminal

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

def get_workspaces(auth_header):
    url = f'https://api.fabric.microsoft.com/v1/workspaces'
    r = requests.get(url, headers=auth_header)
    if r.status_code != 200:
        print(HTML("<ansired><b>ERROR:</b> .</ansired>"))
        print(r.text)
        print(r.status_code)
        sys.exit(1)
    values = json.loads(r.text).get("value")
    return [value.get("id") for value in values]

def create_lakehouse(auth_header, workspace_id, item_definition):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses'
    r = requests.post(url, headers=auth_header, data=item_definition)
    if r.status_code != 201:
        print(HTML("<ansired><b>ERROR:</b> could not create lakehouse.</ansired>"))
        print(r.text)
        print(r.status_code)
        sys.exit(1)
    return r.status_code


def get_lakehouse_id(auth_header, workspace_id, display_name):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses'
    r = requests.get(url, headers=auth_header)
    values = json.loads(r.text).get("value")
    lh = next((item for item in values if item['type'] == 'Lakehouse' and item['displayName'] == display_name), None)
    if not lh:
        print(HTML(f'<ansired><b>ERROR:</b> No lakehouse with display_name {display_name} found in workspace {workspace_id}</ansired>'))
        sys.exit(1)
    return lh['id']

def delete_lakehouse(auth_header, workspace_id, lakehouse_id):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}'
    r = requests.delete(url, headers=auth_header)
    if r.status_code != 201:
        print(HTML("<ansired><b>ERROR:</b> could not delete lakehouse.</ansired>"))
        print(r.text)
        print(r.status_code)
        sys.exit(1)
    return r.status_code