from prefect import flow, task
from transkribus_tasks import (
    get_session_id,
    get_collections,
    get_documents_in_collection,
    filter_new_documents,
    get_page_ids,
    start_layout_analysis,
    start_ocr,
    wait_for_jobs,
    export_and_download,
    upload_all_documents,
    upload_to_transkribus_via_ftp,
    filter_new_documents,
    wait_for_documents_to_appear
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
    upload_to_transkribus_via_ftp(session_id, ftp_username, ftp_password, collection_id, local_upload_path)
    #upload_all_documents(session_id, collection_id, local_upload_path)
    return upload_all_documents(session_id, collection_id, local_upload_path)

@task
def wait_for_documents_to_appear_task(session_id, collection_id, expected_titles):
    return wait_for_documents_to_appear(session_id, collection_id, expected_titles)

@task
def filter_new_docs_task(session_id, col_id, doc_ids):
    return filter_new_documents(session_id, col_id, doc_ids)

@task
def fetch_collections(session_id):
    return get_collections(session_id)

@task
def fetch_documents(session_id, col_id):
    return get_documents_in_collection(session_id, col_id)

@task
def filter_new_docs_task(session_id, col_id, doc_ids):
    return filter_new_documents(session_id, col_id, doc_ids)


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
    issue_id = None  # None in case issues aren't used

    try:
        session_id = login()

        collection_id_for_upload = 1992893
        local_upload_path = "upload"  
        uploaded_titles = upload_documents_task(session_id, collection_id_for_upload, local_upload_path)

        wait_for_completion(session_id, None)  # wait for upload process
        wait_for_documents_to_appear_task(session_id, collection_id_for_upload, uploaded_titles)
        collections = fetch_collections(session_id)
        for col_id, col_name in collections:
            documents = fetch_documents(session_id, col_id)
            all_doc_ids = [doc.get("docId") for doc in documents]
            new_doc_ids = filter_new_docs_task(session_id, col_id, all_doc_ids)
            print(new_doc_ids)
            for doc_id in new_doc_ids:
                page_ids = fetch_page_ids(session_id, col_id, doc_id)
                analyze_layout(session_id, col_id, doc_id, page_ids)
                wait_for_completion(session_id, doc_id)  # wait for lajob
                perform_ocr(session_id, col_id, doc_id, page_ids)
                wait_for_completion(session_id, doc_id)  # wait for ocr
                export_doc(session_id, col_id, doc_id)

        # close_gitlab_issue(issue_id, success_message="Workflow concluded successfully. All documents processed.")

    except Exception as e:
        # update_gitlab_issue(issue_id, f"Error in workflow: {str(e)}")
        raise

if __name__ == "__main__":
    transkribus_workflow()
