import os
import requests

# Author: Ruslana Kruk
# This script manages user authorization, token storage, and validation for a REST API.

# Constants for API and directory paths
auth_api_url = "enter_auth_api_url"
user_api_url_template = "enter_user_api_url_template"  # Template with placeholder for user_id
tokens_directory = "enter_tokens_directory"  # Directory to store token files

# Authorize user and retrieve tokens and cookies
def authorize_user(identifier, password, user_agent):
    headers = {'User-Agent': user_agent}
    params = {"identifier": identifier, "password": password, "locale": "en"}

    with requests.Session() as session:
        response = session.post(auth_api_url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            x_auth_token = data.get("credentials", {}).get("token")
            x_auth_resource = data.get("credentials", {}).get("resource")
            return True, x_auth_token, x_auth_resource, session.cookies
        else:
            return False, None, None, None

# Check validity of the authentication token
def check_auth_token(user_id, x_auth_token, x_auth_resource, user_agent):
    user_api_url = user_api_url_template.format(user_id=user_id)
    headers = {'User-Agent': user_agent, "X-Auth-Token": x_auth_token, "X-Auth-Resource": x_auth_resource}
    response = requests.get(user_api_url, headers=headers)
    return response.status_code == 200

# Save tokens and cookies to files
def save_tokens_and_cookies(user_id, x_auth_token, x_auth_resource, cookies):
    if not os.path.exists(tokens_directory):
        os.makedirs(tokens_directory)

    token_file = os.path.join(tokens_directory, f"{user_id}_token.txt")
    with open(token_file, "w") as f:
        f.write(f"X-Auth-Token: {x_auth_token}\nX-Auth-Resource: {x_auth_resource}\n")

    cookie_file = os.path.join(tokens_directory, f"{user_id}_cookies.txt")
    with open(cookie_file, "w") as f:
        for cookie in cookies:
            f.write(f"{cookie.name}={cookie.value}\n")

# Read tokens and cookies from files
def read_tokens_and_cookies(user_id):
    token_file = os.path.join(tokens_directory, f"{user_id}_token.txt")
    cookie_file = os.path.join(tokens_directory, f"{user_id}_cookies.txt")

    x_auth_token = x_auth_resource = None
    cookies = {}

    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            lines = f.readlines()
            x_auth_token = lines[0].split(":")[1].strip()
            x_auth_resource = lines[1].split(":")[1].strip()

    if os.path.exists(cookie_file):
        with open(cookie_file, "r") as f:
            cookies = {line.split("=")[0]: line.split("=")[1].strip() for line in f if "=" in line}

    return x_auth_token, x_auth_resource, cookies

# Run authorization process
def run_authorization(identifier, password, user_agent):
    user_id = identifier  # Assuming identifier is the user ID
    x_auth_token, x_auth_resource, cookies = read_tokens_and_cookies(user_id)

    if x_auth_token and x_auth_resource:
        if check_auth_token(user_id, x_auth_token, x_auth_resource, user_agent):
            print("Tokens are valid.")
        else:
            print("Tokens are invalid. Reauthorizing...")
            success, x_auth_token, x_auth_resource, cookies = authorize_user(identifier, password, user_agent)
            if success:
                save_tokens_and_cookies(user_id, x_auth_token, x_auth_resource, cookies)
                print("New tokens saved.")
            else:
                print("Authorization failed.")
    else:
        success, x_auth_token, x_auth_resource, cookies = authorize_user(identifier, password, user_agent)
        if success:
            save_tokens_and_cookies(user_id, x_auth_token, x_auth_resource, cookies)
            print("Tokens saved.")
        else:
            print("Authorization failed.")
