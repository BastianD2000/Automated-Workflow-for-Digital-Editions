# This file contains helper tasks that are used all over the place in the project.
# -handling missing files
# - validating xml-files
# - post-processing function

from lxml import etree
from prefect import task
from error_codes import VALIDATION_SUCCESS,VALIDATION_FAILED, VALIDATION_EXCEPTION
from git_tasks import create_issue_on_gitlab

@task
def does_not_exist(file_path):
    '''handles files that do not exist'''
    print(f'{file_path} does not exist')




@task
def validate_xml_with_rng(file_path, rng, file_id):
    '''Validates an XML file against a RelaxNG schema.'''
    try:
        # Parse the XML file
        xml_doc = etree.parse(file_path)

        # Parse the RelaxNG schema
        with open(rng, 'r') as rng_file:
            relaxng_doc = etree.parse(rng_file)
            relaxng = etree.RelaxNG(relaxng_doc)

        # Validate the XML file
        # Invalid File - aborts the flow and creates an issue on GitLab
        if not relaxng.validate(xml_doc):
            error_log = relaxng.error_log
            error_messages = "\n".join([f"Line {error.line}: {error.message}" for error in error_log])
            short_error = error_log[0].message if error_log else "Unknown error"
            todo_list = "\n".join([f"- [ ] Line {error.line}: {error.message}" for error in error_log])
            print(f"XML file {file_path} is not valid. Errors:\n{error_messages}")

            # Create GitLab issue
            issue_title = f"{VALIDATION_FAILED}: Validation failed - {file_id}"
            issue_description = f"Validation errors:\n{todo_list}"
            create_issue_on_gitlab(issue_title, issue_description)

            return False, f"{VALIDATION_FAILED}\n{todo_list}"

        # valid file - prints success message and returns true
        print(f"XML file {file_path} is valid against the schema {rng}.")
        return True, VALIDATION_SUCCESS


    except Exception as e:
        print(f"An error occurred during validation: {e}")
        return False, VALIDATION_EXCEPTION

@task
def post_proc():
    '''handles processing of files with xslt-scripts
    sends them to saxon.jar and takes the result.
    Takes all the files that are specified in the post_proc.xml.
    Should probably take the file from zeid-nas/Projekt-Name/{export-name}/export and put it into  zeid-nas/Projekt-Name/{export-name}/post_proc
    '''
    pass


# <post_proc>
#     <step_1>...</step_1>
#     <step_2>...</step_2>
#     <step_3>...</step_3>
# </post_proc>
