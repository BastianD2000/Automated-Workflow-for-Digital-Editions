from prefect import flow, task
from transkribus_tasks import (
    get_session_id,
    get_collections,
    get_documents_in_collection,
    get_page_ids,
    start_layout_analysis,
    start_ocr,
    wait_for_jobs,
    export_and_download,
    upload_all_documents,
    upload_to_transkribus_via_ftp  
)

"""
from git_issues.git_tasks import (
    create_issue_on_gitlab,
    update_gitlab_issue,
    close_gitlab_issue
)
"""
import os

@task
def login():
    return get_session_id()

@task
def upload_documents_task(session_id, collection_id, local_upload_path):
    ftp_username = os.getenv("TRANSKRIBUS_EMAIL")
    ftp_password = os.getenv("TRANSKRIBUS_PASSWORD")
    #upload_to_transkribus_via_ftp(session_id, ftp_username, ftp_password, collection_id, local_upload_path)
    upload_all_documents(session_id, collection_id, local_upload_path)


@task
def fetch_collections(session_id):
    return get_collections(session_id)

@task
def fetch_documents(session_id, col_id):
    return get_documents_in_collection(session_id, col_id)

@task
def fetch_page_ids(session_id, col_id, doc_id):
    return get_page_ids(session_id, col_id, doc_id)

@task
def analyze_layout(session_id, col_id, doc_id, page_ids):
    start_layout_analysis(session_id, col_id, doc_id, page_ids)

@task
def perform_ocr(session_id, col_id, doc_id, page_ids):
    start_ocr(session_id, col_id, doc_id, page_ids)

@task
def wait_for_completion(session_id, doc_id):
    wait_for_jobs(session_id, doc_id)

@task
def export_doc(session_id, col_id, doc_id):
    export_and_download(session_id, col_id, doc_id)

@flow
def transkribus_workflow():
    # issue_id = create_issue_on_gitlab(
    #     title="Transkribus Flow started",
    #     description="Workflow with upload and complete processing."
    # )
    issue_id = None  # Dummy-Wert

    try:
        session_id = login()

        collection_id_for_upload = 1992893
        local_upload_path = "Test_Texte"  # Oder absoluten Pfad angeben
        upload_documents_task(session_id, collection_id_for_upload, local_upload_path)

        wait_for_completion(session_id, None)  # Warten auf Upload-Prozess (UploadJob)

        collections = fetch_collections(session_id)
        for col_id, col_name in collections:
            documents = fetch_documents(session_id, col_id)
            for doc in documents:
                doc_id = doc.get("docId")
                page_ids = fetch_page_ids(session_id, col_id, doc_id)
                analyze_layout(session_id, col_id, doc_id, page_ids)
                wait_for_completion(session_id, doc_id)  # Warten auf LAJob
                export_doc(session_id, col_id, doc_id)

        # close_gitlab_issue(issue_id, success_message="Workflow erfolgreich abgeschlossen. Alle Dokumente verarbeitet.")

    except Exception as e:
        # update_gitlab_issue(issue_id, f"Fehler im Workflow: {str(e)}")
        raise

if __name__ == "__main__":
    transkribus_workflow()
