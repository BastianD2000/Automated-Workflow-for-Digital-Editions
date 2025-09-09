import os
import requests as r
from requests.auth import HTTPBasicAuth
import os
from prefect import task
from git_tasks import create_issue_on_gitlab, update_gitlab_issue, close_gitlab_issue
import tempfile
from dotenv import load_dotenv
from helper_tasks import validate_xml_with_rng
from error_codes import FILE_FETCH_SUCCESS, FILE_FETCH_FAILED, UPLOAD_SUCCESS, UPLOAD_FAILED, UPLOAD_VALIDATION_FAILED


load_dotenv()  # Load environment variables

#load global vars needed for exist-tasks
EXIST_SERVER = os.getenv("exist_server") # target-server for the upload
EXIST_USER = os.getenv("exist_user")
EXIST_PASSWORD = os.getenv("exist_password")
RELAXNG_SCHEMA_PATH = os.getenv("RELAXNG_SCHEMA_PATH") # should be a path to the file on the server, where the RelaxNG schema is stored.
fetch_server = "https://exist.ulb.tu-darmstadt.de/2/g/"

# exist_server:  exist_server="https://exist.ulb.tu-darmstadt.de/3/r/edoc/collection/"

def create_version_of_file():
    '''creates a version of a file in the wdbplus collection,
    by pushing the old one to github/lab and collecting the git-hash.
    Should also go into the file and add a version number to the file, as well as adding a pointer to the old version?
    '''
    pass


def get_file_from_server(fetch_server, id_to_get):
    '''fetches a file from an exist-server and saves it temporarily
    args:
    id_to_get: str: the id of the file to get, on the server from which the file is taken.
    still needs fetch_server to construct the url dynamically
    '''
    server_url = fetch_server
    id_to_get = id_to_get
    response = r.get(server_url+id_to_get)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xml")
    if response.status_code == 200:
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f"{id_to_get}.xml")

        with open(temp_path, "wb") as temp_file:
            temp_file.write(response.content)
        print(f"DEBUG: File saved temporarily at: {temp_path}")

        return temp_path, FILE_FETCH_SUCCESS

    print(f"ERROR: Failed to fetch file {id_to_get}, status: {response.status_code}")
    return None, FILE_FETCH_FAILED  # <-- ONLY RETURN NONE IN ERROR CASES


@task
def update_or_create_file(fetch_server, target_server, collection, id_to_get):
    '''
    Tries fetching a file from an exist-db server, validates it against a RelaxNG schema, and uploads it to the server.
    Documents each step in the process by creating a GitLab issue and updating it accordingly.
    fetch_server should be the same as in get_file_from_server
    Args:
        fetch_server (str): The server from which the file is fetched - not used, yet.
        target_server (str): The server to which the file is uploaded - not used, yet.
        collection (str): The collection in which the file is stored.
        id_to_get (str): The ID of the file to get from the source server.

    Returns:
        str: Status message indicating success or failure.
    '''
    try:
        file_path, fetch_status = get_file_from_server(fetch_server=fetch_server, id_to_get=id_to_get)
    except Exception as e:
        issue_id = create_issue_on_gitlab(
            title=f"Error fetching {id_to_get} from {fetch_server}",
            description=f"Exception occurred while fetching file {id_to_get}: {str(e)} - from {fetch_server}"
        )
        print(f"Error fetching file {id_to_get}: {e}")
        return FILE_FETCH_FAILED

    if fetch_status != FILE_FETCH_SUCCESS:
        issue_id = create_issue_on_gitlab(
            title=f"{id_to_get} not found on {fetch_server}",
            description=f"Could not fetch file with ID {id_to_get} from the server {fetch_server}"
        )
        print(fetch_status)
        return fetch_status

    # Validate the XML file
    try:
        is_valid, validation_status = validate_xml_with_rng(file_path,RELAXNG_SCHEMA_PATH, id_to_get)
        if not is_valid:
            issue_id = create_issue_on_gitlab(
                title=f"Validation failed for {id_to_get}",
                description=f"Validation errors:\n{validation_status}"
            )
            print("Validation failed")
            return UPLOAD_VALIDATION_FAILED
    except Exception as e:
        issue_id = create_issue_on_gitlab(
            title=f"Validation error for {id_to_get}",
            description=f"Exception occurred while validating {id_to_get}: {str(e)}"
        )
        print(f"Validation error: {e}")
        return UPLOAD_VALIDATION_FAILED

    # Construct upload URL
    url = EXIST_SERVER + collection
    file_name = os.path.basename(file_path)
    target_path = f"texts/{file_name}"

    issue_title = f"Processing file: {file_name}"
    issue_description = f"Starting upload/update for {file_name} in collection {collection}."
    issue_id = create_issue_on_gitlab(issue_title, issue_description)

    try:
        check_response = r.head(f"{url}/resources/{file_name}")
        if check_response.status_code == 200:
            print(f"File {file_name} already exists. Updating file...")
            # Implement file update logic here
            update_message = f"File {file_name} exists. Update in progress."
            update_gitlab_issue(issue_id, update_message)
        else:
            print(f"Uploading {file_name} to {target_path}...")
            with open(file_path, 'rb') as file:
                files = {'file': (file_name, file, 'application/xml')}
                data = {'filename': target_path}

                response = r.post(url, files=files, data=data,
                                  auth=HTTPBasicAuth(username=EXIST_USER, password=EXIST_PASSWORD))

                if response.status_code in [200, 201]:
                    success_message = f"{UPLOAD_SUCCESS}: Uploaded file {file_name} successfully."
                    close_gitlab_issue(issue_id, success_message)
                    print(success_message)
                    return UPLOAD_SUCCESS
                else:
                    error_message = f"{UPLOAD_FAILED}: Failed to upload file {file_name}. Status: {response.status_code}"
                    update_gitlab_issue(issue_id, error_message)
                    print(f"Failed: {file_path}")
                    print(f"Status code: {response.status_code}")
                    print(f"Response: {response.text}")
                    return UPLOAD_FAILED

    except Exception as e:
        error_message = f"{UPLOAD_FAILED}: Exception occurred while uploading {file_name}: {str(e)}"
        update_gitlab_issue(issue_id, error_message)
        print(f"Exception during upload: {e}")
        return UPLOAD_FAILED


@task
def push_to_exist(fetch_server,target_server, collection, id_to_get):
    '''pushes {file_path} to exist-db db'''
    update_or_create_file(fetch_server=fetch_server, target_server=EXIST_SERVER, collection=collection, id_to_get=id_to_get)
