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

    test = req.params.get('test')
    if not test:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            test = req_body.get('test')

    try:
        result = optimo_post_jobs.main(test=test)

        if not result['success'].empty:
            success_df_str = result['success'].to_string(index=False)
        else:
            success_df_str = ''

        if not result['fail'].empty:
            fail_df_str = result['fail'].to_string(index=False)
        else:
            fail_df_str = ''

        return  func.HttpResponse(
    f"""RESULT: {result['message']}
Planning success: {result['plannig']}
\n\n\n\n\nFail Jobs: {result['fail'].shape[0]}
{fail_df_str}
\n\n\n\n\nSuccess Jobs: {result['success'].shape[0]}
{success_df_str}""",
            status_code=200)
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse(
            f"This HTTP triggered function FAIL .\n\n{str(e)}",
            status_code=500
        )