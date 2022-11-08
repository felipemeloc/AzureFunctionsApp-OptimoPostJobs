from . import load_env

import os
import sys
import logging
import azure.functions as func
# from dotenv import load_dotenv

# Load environment variables
# load_dotenv()
# Define project main path
MAIN_FOLDER = os.getenv('MAIN_PATH')

sys.path.insert(0, os.path.join(os.getcwd(), os.path.join(MAIN_FOLDER, "src") ))

from . import optimo_post_jobs


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        message, planning_status, success_df, fail_df = optimo_post_jobs.main()

        if not success_df.empty:
            success_df_str = success_df.to_string(index=False)
        else:
            success_df_str = ''

        if not fail_df.empty:
            fail_df_str = fail_df.to_string(index=False)
        else:
            fail_df_str = ''

        return  func.HttpResponse(
f"""RESULT: {message}
Planning success: {planning_status}
\nFail Jobs: {success_df.shape[0]}
{fail_df_str}
\nSuccess Jobs: {fail_df.shape[0]}
{fail_df_str}""",
        status_code=200)
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse(
            f"This HTTP triggered function FAIL .\n\n{str(e)}",
            status_code=500
        )