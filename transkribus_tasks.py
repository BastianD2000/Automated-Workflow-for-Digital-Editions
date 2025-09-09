import requests
import json
import os
import time
from lxml import etree
from dotenv import load_dotenv
import logging
import xml.etree.ElementTree as ET
import hashlib
import urllib.parse
from ftplib import FTP
import zipfile

BASE_URL = "https://transkribus.eu/TrpServer/rest"
load_dotenv()

# Logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_to_transkribus_via_ftp(session_id, ftp_username, ftp_password, collection_id, local_dir):
    """
    Uploads files to Transkribus via FTP and triggers ingestion into a specified collection.
    Expects a valid session ID (e.g., from get_session_id).

    :param session_id: Active Transkribus session ID
    :param ftp_username: Transkribus FTP username
    :param ftp_password: Transkribus FTP password
    :param collection_id: ID of the target collection in Transkribus
    :param local_dir: Full path to the local upload directory
    :return: List of uploaded document titles if successful, otherwise []
    """

    try:
        headers = {"Cookie": f"JSESSIONID={session_id}"}
        session = requests.Session()
        session.headers.update(headers)

        # create ftp-connection
        ftp = FTP("transkribus.eu")
        ftp.login(user=ftp_username, passwd=ftp_password)

        if not os.path.exists(local_dir):
            logger.error(f"[!] Local directory couldn't be found: {local_dir}")
            return []

        shorthand = os.path.basename(local_dir)
        ftp_dir = f"/{shorthand}"
        try:
            ftp.cwd(ftp_dir)
        except:
            ftp.mkd(ftp_dir)
            ftp.cwd(ftp_dir)

        for filename in os.listdir(local_dir):
            local_path = os.path.join(local_dir, filename)
            if os.path.isfile(local_path) and filename not in ftp.nlst():
                with open(local_path, "rb") as file:
                    ftp.storbinary(f"STOR {filename}", file)
                logger.info(f"[+] File uploaded: {filename}")
            else:
                logger.info(f"[-] File skipped (already exists): {filename}")

        # start ingest by API
        encoded_name = urllib.parse.quote(shorthand)
        ingest_url = f"{BASE_URL}/collections/{collection_id}/ingest?fileName={encoded_name}"
        ingest_response = session.post(ingest_url)
        ingest_response.raise_for_status()
        logger.info(f"[+] Ingest started for '{shorthand}': {ingest_response.text}")

        return [shorthand]  # return title

    except Exception as e:
        logger.error(f"[!] Error while FTP-Upload: {e}")
        return []



def upload_all_documents(session_id, collection_id, main_dir):
    """
    Uploads all subfolders from a directory to Transkribus. Each subfolder becomes a separate document.

    :param session_id: Active Transkribus session.
    :param collection_id: ID of the collection to upload to.
    :param main_dir: Main directory containing subfolders with image files.
    :return: List of uploaded document titles
    """

    def calculate_md5(file_path):
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    uploaded_titles = []  # collect titles
    for folder_name in sorted(os.listdir(main_dir)):
        folder_path = os.path.join(main_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue

        supported_ext = [".jpg", ".jpeg", ".tif", ".tiff"]
        images = sorted([
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if os.path.splitext(f.lower())[1] in supported_ext
        ])

        if not images:
            print(f"No valid image files found in the directory: {folder_path}")
            continue

        try:
            session = requests.Session()
            session.headers.update({"Cookie": f"JSESSIONID={session_id}"})

            url_create = f"{BASE_URL}/uploads?collId={collection_id}"
            payload = {
                "md": {"title": folder_name},
                "pageList": {
                    "pages": [
                        {
                            "fileName": os.path.basename(img),
                            "pageNr": i + 1,
                            "imgChecksum": calculate_md5(img),
                        }
                        for i, img in enumerate(images)
                    ]
                }
            }

            response = session.post(url_create, json=payload)
            if response.status_code != 200:
                raise Exception(f"Error creating the upload: {response.status_code} - {response.text}")

            root = ET.fromstring(response.text)
            upload_id = root.find("uploadId").text
            print(f"Upload ID received: {upload_id}")

            for img in images:
                url_upload = f"{BASE_URL}/uploads/{upload_id}"
                with open(img, "rb") as img_file:
                    files = {"img": (os.path.basename(img), img_file, "application/octet-stream")}
                    response = session.put(url_upload, files=files)

                if response.status_code != 200:
                    raise Exception(f"Error uploading the page {img}: {response.status_code} - {response.text}")

                print(f"Page {img} successfully uploaded.")

            print(f"Upload of '{folder_name}' with {len(images)} pages completed successfully.")
            uploaded_titles.append(folder_name)  # save title

        except Exception as e:
            print(f"Error uploading '{folder_name}': {str(e)}")

    print("Upload process completed.")
    return uploaded_titles  # return list of titles



def wait_for_documents_to_appear(session_id, collection_id, expected_titles, timeout=300, poll_interval=5):
    """
    Waits until all expected document titles are visible in the collection list.
    Returns a list of tuples (title, docId).

    """
    start = time.time()
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    url = f"{BASE_URL}/collections/{collection_id}/list"

    expected = set(expected_titles)
    found = set()

    while time.time() - start < timeout:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        docs = r.json()

        seen = {
            (d.get("title") or d.get("md", {}).get("title")): d.get("docId")
            for d in docs
        }
        found = {t for t in expected if t in seen}

        if found == expected:
            return [(t, seen[t]) for t in expected_titles]

        time.sleep(poll_interval)

    raise TimeoutError(f"Documents did not appear in the collection in time: {expected - found}")



def filter_new_documents(session_id, collection_id, doc_ids):
    """
    Checks a list of documents to determine which have the status "New"
    and returns only those document IDs.

    :param session_id: Transkribus session ID
    :param collection_id: ID of the collection
    :param doc_ids: List of document IDs
    :return: List of document IDs with status "New"
    """
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    new_doc_ids = []

    for doc_id in doc_ids:
        url = f"{BASE_URL}/collections/{collection_id}/{doc_id}/fulldoc"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logger.warning(f"Document {doc_id} couldn't be loaded: {response.status_code}")
            continue

        try:
            data = response.json()
            md = data.get("md", {})
            if md.get("nrOfNew", 0) > 0:
                new_doc_ids.append(doc_id)

        except Exception as e:
            logger.warning(f"Error while processing the document {doc_id}: {e}")
            continue

    logger.info(f"[+] Found 'New'-documents: {new_doc_ids}")
    return new_doc_ids



def get_session_id():
    """
    Performs the login to Transkribus and returns the session ID.
    """
    email = os.getenv("TRANSKRIBUS_EMAIL")
    password = os.getenv("TRANSKRIBUS_PASSWORD")
    
    if not email or not password:
        raise Exception("Missing login credentials: Please ensure that the .env file is correct.")
    
    session = requests.Session()
    response = session.post(f"{BASE_URL}/auth/login", data={"user": email, "pw": password})

    if response.status_code != 200:
        raise Exception(f"Login failed: {response.status_code} - {response.text}")

    try:
        root = etree.fromstring(response.content)
        session_id = root.find("sessionId").text
        print("Successfully logged in! Session-ID:", session_id)
        return session_id
    except etree.ParseError:
        raise Exception("Error: The XML response could not be parsed.")



def get_collections(session_id):
    """Fetches all collections as a list of tuples (ID, Name)."""
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    response = requests.get(f"{BASE_URL}/collections/list", headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error retrieving the collections: {response.status_code} - {response.text}")

    try:
        data = response.json()
        collections = [(col["colId"], col["colName"]) for col in data]
        return collections
    except ValueError as e:
        raise Exception(f"JSON-parsing-error: {e}")



def get_documents_in_collection(session_id, collection_id):
    """Fetches all documents from a specific collection."""
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    url = f"{BASE_URL}/collections/{collection_id}/list"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error while retrieving the documents: {response.status_code} - {response.text}")

    try:
        data = response.json()
        return data
    except ValueError as e:
        raise Exception(f"JSON-parsing-error: {e}")



def get_page_ids(session_id, collection_id, doc_id):
    """Fetch pageIds for a specific document via the /fulldoc endpoint."""
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    url = f"{BASE_URL}/collections/{collection_id}/{doc_id}/fulldoc"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Error when retrieving pages for document {doc_id}: {response.status_code}")
        return []
    try:
        data = response.json()
        pages = data.get("pageList", {}).get("pages", [])
        page_ids = [page["pageId"] for page in pages]
        return page_ids
    except Exception as e:
        logger.error(f"Error when parsing the pages: {e}")
        return []



def start_layout_analysis(session_id, collection_id, doc_id, page_ids):
    """Starts the layout analysis for the specified document, without requiring tsIds."""

    def json_to_xml_description(doc_id, page_ids):
        try:
            root = etree.Element("documentSelectionDescriptors")
            doc_desc = etree.SubElement(root, "documentSelectionDescriptor")

            doc_id_el = etree.SubElement(doc_desc, "docId")
            doc_id_el.text = str(doc_id)

            page_list_el = etree.SubElement(doc_desc, "pageList")
            for page_id in page_ids:
                pages_el = etree.SubElement(page_list_el, "pages")
                page_id_el = etree.SubElement(pages_el, "pageId")
                page_id_el.text = str(page_id)

                region_ids_el = etree.SubElement(pages_el, "regionIds")
                region_ids_el.text = ""

            return etree.tostring(root, encoding='utf-8', pretty_print=True)
        except Exception as e:
            logger.error(f"[!] Error generating XML description: {e}")
            raise

    url = f"{BASE_URL}/LA/analyze"
    cookies = {"JSESSIONID": session_id}

    params = {
        "collId": collection_id,
        "doBlockSeg": "true",
        "doLineSeg": "true",
        "doWordSeg": "false",
        "doPolygonToBaseline": "false",
        "doBaselineToPolygon": "false",
        "jobImpl": "CITlabAdvancedLaJob",
        "credits": "AUTO",
    }

    try:
        xml_desc = json_to_xml_description(doc_id, page_ids)
        headers = {'Content-Type': 'application/xml'}
        response = requests.post(url, cookies=cookies, params=params, data=xml_desc, headers=headers)

        if response.status_code == 200:
            logger.info(f"[+] Layout analysis for document {doc_id} started.")
        else:
            logger.error(f"[-] Error starting layout analysis: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"[!] Request failed: {e}")
    except Exception as e:
        logger.error(f"[!] Unexpected error: {e}")



def wait_for_jobs(session_id, doc_id=None, job_types=("LAJob", "TextRecognitionJob", "UploadJob"), poll_interval=5):
    """
    Waits until all relevant jobs are completed. If doc_id is None, it waits for all active jobs.
    """
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    job_list_url = f"{BASE_URL}/jobs/list"

    print("Wait for Transkribus-Jobs...")

    while True:
        response = requests.get(job_list_url, headers=headers)
        if response.status_code != 200:
            print(f"Error retrieving job status: {response.status_code} - {response.text}")
            return

        jobs = response.json()

        # Filter for relevant jobs
        relevant_jobs = [
            job for job in jobs
            if (doc_id is None or job.get("docId") == doc_id)
            and job.get("state") != "FINISHED"
            and job.get("jobType") in job_types
        ]

        if not relevant_jobs:
            print("All relevant jobs completed.")
            break

        print(f"Open Jobs: {[ (j['jobId'], j['jobType'], j['state']) for j in relevant_jobs ]}")
        time.sleep(poll_interval)



def start_ocr(session_id, collection_id, doc_id, page_ids): # Doesn't work yet!
    """
    Starts OCR via /recognition/ocr using the legacy OCR engine.
    """
    import logging
    import requests

    logger = logging.getLogger(__name__)
    url = f"{BASE_URL}/recognition/ocr"
    cookies = {"JSESSIONID": session_id}

    params = {
        "collId": collection_id,
        "id": doc_id,
        "pages": ",".join(str(pid) for pid in page_ids),
        "type": "Legacy",  # Legacy OCR-Engine
    }

    response = requests.post(url, cookies=cookies, params=params)

    if response.status_code == 200:
        logger.info(f"[+] OCR started for document {doc_id}")
    else:
        logger.error(f"[-] Error starting OCR: {response.status_code} - {response.text}")



def export_and_download(session_id, collection_id, document_id):
    """Exports, downloads, and extracts the document after processing."""
    headers = {"Cookie": f"JSESSIONID={session_id}"}
    url = f"{BASE_URL}/collections/{collection_id}/{document_id}/export"
    response = requests.post(url, headers=headers, json={"format": "application/zip"})

    try:
        response_data = response.json()
    except requests.exceptions.JSONDecodeError:
        response_data = response.text.strip()
        if response_data.isdigit():
            job_id = int(response_data)
        else:
            print(f"Error starting the export: {response.text}")
            return
    else:
        job_id = response_data.get("jobId") if isinstance(response_data, dict) else response_data

    if not job_id:
        print(f"Error starting the export: {response.status_code} - {response.text}")
        return

    print(f"Export job started! Job ID: {job_id}")

    # Wait for export to complete
    while True:
        time.sleep(5)
        status_url = f"{BASE_URL}/jobs/{job_id}"
        status_response = requests.get(status_url, headers=headers)

        if status_response.status_code != 200:
            print(f"Error retrieving job status: {status_response.status_code} - {status_response.text}")
            return

        status_data = status_response.json()
        if status_data.get("state") == "FINISHED":
            download_url = status_data.get("result")
            if download_url:
                print("Export completed. Download URL is available.")
                break
        else:
            print(f"Export status: {status_data.get('state')}. Waiting for completion...")

    # Download the file
    response = requests.get(download_url, stream=True)
    if response.status_code == 200:
        os.makedirs("downloads", exist_ok=True)
        zip_path = os.path.join("downloads", f"export_{job_id}.zip")
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Download completed: {zip_path}")

        # Extract ZIP
        extract_dir = os.path.join("downloads", f"export_{job_id}")
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"ZIP extracted to: {extract_dir}")

    else:
        print(f"Error downloading: {response.status_code} - {response.text}")

