import os
import gitlab
from github import Github
from prefect import task
import base64

from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables
GITHUB_REPO = "WunschK/TEEEEST"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/issues"

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_URL = "https://gitlab.ulb.tu-darmstadt.de"
GITLAB_REPO_ID = "KWunsch/workflow-tests"

GITLAB_ISSUE_REPO_ID = "zeid/prefect-automation-issues"
ISSUE_GITLAB_TOKEN = os.getenv("ISSUE_GITLAB_TOKEN")
GITLAB_ISSUE_PROJECT_ID = 692

# Define tasks to be executed in "distribute_stuff"

@task
def create_issue_on_gitlab(title, description):
    '''Creates an issue in GitLab project ID 692 and returns its ID'''
    print("Starting create_issue_on_gitlab task...")
    max_title_length = 255
    if len(title) > max_title_length:
        title = title[:max_title_length]
    gl = gitlab.Gitlab(GITLAB_URL, private_token=ISSUE_GITLAB_TOKEN)
    project = gl.projects.get(GITLAB_ISSUE_PROJECT_ID)
    issue = project.issues.create({'title': title, 'description': description})

    print(f"Issue {issue.iid} created successfully!")
    print("Exiting create_issue_on_gitlab task...")

    return issue.iid


@task
def update_gitlab_issue(issue_id, message):
    '''Updates an issue in GitLab with a new comment'''
    gl = gitlab.Gitlab(GITLAB_URL, private_token=ISSUE_GITLAB_TOKEN)
    project = gl.projects.get(GITLAB_ISSUE_PROJECT_ID)
    issue = project.issues.get(issue_id)

    # Append a new message to the issue description
    issue.description += f"\n\n{message}"
    issue.save()
    print(f"Updated issue {issue_id} with message: {message}")

@task
def close_gitlab_issue(issue_id, success_message):
    '''Updates the issue description and closes the given GitLab issue'''
    gl = gitlab.Gitlab(GITLAB_URL, private_token=ISSUE_GITLAB_TOKEN)
    project = gl.projects.get(GITLAB_ISSUE_PROJECT_ID)
    issue = project.issues.get(issue_id)

    # Append success message and close the issue
    issue.description += f"\n\n{success_message}"
    issue.state_event = "close"
    issue.save()
    print(f"Issue {issue_id} updated and closed: {success_message}")




@task
def copy_to_gitlab(file_path, subdir):
    '''Copies {file_path} to GitLab repository and manages an issue in project 692'''

    # Use GITLAB_TOKEN for file upload project in the project repo (workflow-tests)
    gl_file_project = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    project = gl_file_project.projects.get(GITLAB_REPO_ID)

    file_path_in_repo = f"{subdir}/{file_path}"

    # Create issue in project for issue management repo (prefect-automation-issues)
    issue_id = create_issue_on_gitlab.fn(
        title=f"Upload {file_path_in_repo} to Gitlab",
        description=f"Started upload of {file_path_in_repo} to GitLab-Repo {project.name}"
    )
    print(f"Issue created: {issue_id}")

    try:
        with open(file_path, 'r') as file:
            content = file.read()

        # Try updating the file first
        try:
            file = project.files.get(file_path=file_path_in_repo, ref='main')
            file.content = content
            file.save(branch='main', commit_message="Updating file")
            success_message = f"Updated file {file_path_in_repo} on GitLab"
            print(success_message)

        except gitlab.exceptions.GitlabGetError:
            # If file does not exist, create it
            project.files.create({
                'file_path': file_path_in_repo,
                'branch': 'main',
                'content': content,
                'commit_message': 'Adding new file'
            })
            success_message = f"Created new file {file_path_in_repo} on GitLab"
            print(success_message)

        # Update and close the GitLab issue with the success message
        update_gitlab_issue.fn(issue_id, success_message)
        close_gitlab_issue.fn(issue_id, success_message)

    except Exception as e:
        error_message = f"Failed to create/update file {file_path_in_repo}: {str(e)}"
        print(error_message)

        # Use update_gitlab_issue to append the error message
        update_gitlab_issue.fn(issue_id, error_message)


@task
def copy_to_github(file_path, subdir):
    '''Copies {file_path} to GitHub repository and logs to GitLab issue tracker'''
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(GITHUB_REPO)

    file_name = os.path.basename(file_path)
    github_path = f"{subdir}/{file_name}"

    # Create an issue in the GitLab tracking repo
    issue_id = create_issue_on_gitlab(
        title=f"Upload {github_path} to GitHub",
        description=f"Started upload of `{github_path}` to GitHub"
    )

    with open(file_path, 'r') as file:
        content = file.read()

    try:
        # Try updating an existing file
        contents = repo.get_contents(github_path)
        repo.update_file(contents.path, "Updating file", content, contents.sha)
        success_message = f"Updated file {github_path} on GitHub"
        print(success_message)

    except Exception:
        try:
            # Create a new file if it doesn't exist
            repo.create_file(github_path, "Adding new file", content)
            success_message = f"Created new file {github_path} on GitHub"
            print(success_message)

        except Exception as e:
            # Handle errors and log them in GitLab
            error_message = f"Failed to create/update file {github_path}: {e}"
            print(error_message)
            update_gitlab_issue(issue_id, error_message)
            return

    # If everything succeeds, update and close the GitLab issue
    close_gitlab_issue(issue_id, success_message)
