import base64
import hashlib
import os
import platform
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

    