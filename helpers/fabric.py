import requests
import json
import sys
from prompt_toolkit import print_formatted_text as print
from prompt_toolkit import HTML
    
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

# lakehouse

def create_lakehouse(auth_header, workspace_id, item_definition):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses'
    r = requests.post(url, headers=auth_header, data=item_definition)
    if r.status_code != 201:
        print(HTML("<ansired><b>ERROR:</b> could not create lakehouse.</ansired>"))
        print(r.text)
        print(r.status_code)
        sys.exit(1)
    return r.status_code

def get_lakehouses(auth_header, workspace_id):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses'
    r = requests.get(url, headers=auth_header)
    if r.status_code != 200:
        print(HTML(f'<ansired><b>ERROR:</b>Error retrieving lakehouses</ansired>'))
        sys.exit(1)
    values = json.loads(r.text).get("value")
    lhs = [item for item in values if item['type'] == 'Lakehouse']
    return lhs

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

# notebook

def create_notebook(auth_header, workspace_id, display_name, nb_content_b64):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items'
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
    r = requests.post(url, headers=auth_header, json=payload)
    if r.status_code != 202:
        print(HTML(f"<ansired><b>ERROR:</b> could not create notebook {display_name}.</ansired>"))
        print(r.text)
        print(r.status_code)
        sys.exit(1)
    return r.status_code


def delete_notebook(auth_header, workspace_id, notebook_id):
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{notebook_id}'

    r = requests.delete(url, headers=auth_header)
    if r.status_code != 200:
        print(HTML(f"<ansired><b>ERROR:</b> could not delete notebook with id {notebook_id}.</ansired>"))
        print(r.text)
        print(r.status_code)
        sys.exit(1)
    return r.status_code

