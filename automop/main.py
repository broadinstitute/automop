import argparse
import json
import flask
from flask import render_template, jsonify, request, session, redirect
from flask import Markup
import asyncio
from concurrent.futures import ThreadPoolExecutor

import google.oauth2.credentials
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2 import id_token
import google.auth
import firecloud.api as fapi
import webbrowser
import threading
import os

app = flask.Flask(__name__)

DRY_RUN = False

def get_user_email_and_groups():
    authorized_session = AuthorizedSession(google.auth.default(['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email'])[0])
    authorized_session.get('https://api.firecloud.org/api/health')
    email = id_token.verify_oauth2_token(authorized_session.credentials.id_token, Request(session=authorized_session), clock_skew_in_seconds=10)['email']
    groups = fapi.get_groups().json()
    return email, groups

@app.route('/')
def index():
    try:
        email, groups = get_user_email_and_groups()
    except Exception as e:
        app.logger.error(str(e))
        return render_template('error.html', error_message=Markup(
            'Something went wrong verifying your Google credentials. Make sure you have valid <a href="https://cloud.google.com/docs/authentication/application-default-credentials">Application Default Credentials</a>, ' +
            'usually this done by running <pre>gcloud auth application-default login</pre> in your terminal.'))
    session['user'] = email

    if not any([group['groupEmail'] == 'automop_users@firecloud.org' for group in groups]):
        return redirect('/missing_group_access')

    return redirect('/workspaces')

@app.route('/missing_group_access')
def missing_group_access():
    return render_template('missing_group_access.html', user=session['user'])

@app.route('/request_group_access')
def request_group_access():
    error_message = Markup('Something went wrong requesting group access. Please slack or email <a href="mgatzen@broadinstitute.org">mgatzen@broadinstitute.org</a> with the error message shown in the console.')
    try:
        fapi_response = fapi.request_access_to_group('automop_users')
        if not fapi_response.ok:
            app.logger.warning(fapi_response.json()["message"])
            return render_template('error.html', error_message=error_message)
    except Exception as e:
        return render_template('error.html', error_message=error_message)
    return render_template('requested_group_access.html')

def get_workspace_cost(workspace):
    result = fapi.get_storage_cost(workspace[0], workspace[1])
    if not result.ok:
        app.logger.warning(f'Failed to get workspace cost for {workspace[0]}/{workspace[1]}: {result.json()["message"]}')
        return 'N/A'
    return result.json()['estimate']

@app.route('/get_workspaces')
async def get_workspaces():
    all_workspaces = fapi.list_workspaces().json()
    writer_workspaces = [(workspace['workspace']['namespace'], workspace['workspace']['name']) for workspace in all_workspaces if workspace['accessLevel'] == 'OWNER']

    workspace_costs = None

    with ThreadPoolExecutor(max_workers=50) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, get_workspace_cost, workspace) for workspace in writer_workspaces]
        workspace_costs = [cost for cost in await asyncio.gather(*tasks)]
    
    workspaces_json = [{'namespace': writer_workspaces[i][0], 'name': writer_workspaces[i][1], 'cost': workspace_costs[i]} for i in range(len(writer_workspaces))]
    
    return jsonify(sorted(workspaces_json, key=lambda workspace: float('+inf') if workspace['cost'] == 'N/A' else float(workspace['cost'][1:])))

@app.route('/workspaces')
def workspaces():
    if 'user' not in session:
        return render_template('error.html', error_message=Markup('The user email was not found in the session. Try navigating to the <a href="/">homepage</a>, which will set this variable.'))

    return render_template('workspaces.html', user=session['user'])

def submit_automop_job(workspace, user):
    workspace_namespace = workspace[0]
    workspace_name = workspace[1]
    method = {
        'methodRepoMethod': {
            'methodName': 'Automop',
            'methodVersion': 16,
            'methodNamespace': 'DSPMethods_mgatzen',
            'methodUri': 'agora://DSPMethods_mgatzen/Automop/16', 'sourceRepo': 'agora'
            },
        'name': 'Automop',
        'namespace': 'DSPMethods_mgatzen',
        'inputs': {
            'Mop.user': f'"{user}"',
            'Mop.workspace_namespace': f'"{workspace_namespace}"',
            'Mop.workspace_name': f'"{workspace_name}"',
            'Mop.dry_run': 'true' if DRY_RUN else 'false',
        },
        'outputs': {},
        'methodConfigVersion': 16,
        'deleted': False
    }
    result = fapi.create_workspace_config(workspace_namespace, workspace_name, method)
    if not result.ok:
        return False, result.json()['message']
    result = fapi.create_submission(workspace_namespace, workspace_name, 'DSPMethods_mgatzen', 'Automop', use_callcache=False, delete_intermediate_output_files=True)
    if not result.ok:
        return False, result.json()['message']
    submission_id = result.json()['submissionId']
    result = fapi.delete_workspace_config(workspace_namespace, workspace_name, 'DSPMethods_mgatzen', 'Automop')
    if not result.ok:
        return False, result.json()['message']
    return True, submission_id


@app.route('/mop', methods=['POST'])
async def mop():
    if 'delete_files' not in request.form or request.form['delete_files'] != 'on':
        return render_template('error.html', error_message=Markup('The confirmation checkbox on the previous page was not checked. Please make sure that you want to permanently delete files using this mop tool.'))
    
    workspaces_to_mop = [tuple(workspace_and_checkbox[0].split('/')[1:]) for workspace_and_checkbox in request.form.items() if workspace_and_checkbox[0].startswith('mop_workspace/') and workspace_and_checkbox[1] == 'on']

    if len(workspaces_to_mop) == 0:
        return render_template('error.html', error_message=Markup('No workspaces were selected to be mopped. Please select at least one workspace.'))

    if 'user' not in session:
        return render_template('error.html', error_message=Markup('The user email was not found in the session. Try navigating to the <a href="/">homepage</a>, which will set this variable.'))
    
    with ThreadPoolExecutor(max_workers=50) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, submit_automop_job, *(workspace, session['user'])) for workspace in workspaces_to_mop]
        submission_results = [result for result in await asyncio.gather(*tasks)]
    
    mop_results = []
    all_ok = True
    for i in range(len(workspaces_to_mop)):
        if submission_results[i][0]:
            status_cell = Markup(f'<a target="_blank" rel="noopener noreferrer" href="https://app.terra.bio/#workspaces/{workspaces_to_mop[i][0]}/{workspaces_to_mop[i][1]}/job_history/{submission_results[i][1]}">View submission</a>')
        else:
            all_ok = False
            status_cell = Markup(f'<span style="color: red;">{submission_results[i][1]}</span>')
        
        mop_results.append({'workspace_namespace': workspaces_to_mop[i][0], 'workspace_name': workspaces_to_mop[i][1], 'status': status_cell})
    if all_ok:
        return render_template('mop.html', mop_status='All mop jobs submitted successfully.', finished_message=Markup('You can now close this tab and terminate the app by pressing <code>Control + C</code> in the command line.'), mop_results=mop_results)
    else:
        return render_template('mop.html', mop_status='There have been errors submitting the mop jobs:', finished_message=Markup('If the cause is not obvious, please send the error messages shown below to <a href="mailto:mgatzen@broadinstitute.org">mgatzen@broadinstitute.org</a>.'), mop_results=mop_results)

def open_browser(host, port):
    print("Starting local web browser")
    url = f'http://{host}:{port}'
    webbrowser.open_new(url)

def get_cli():
    parser = argparse.ArgumentParser(prog='automop-webui', description='Automop Web UI')
    parser.add_argument('--host', default='127.0.0.1', help='IP address for webserver to listen on')
    parser.add_argument('--port', default=8080, type=int, help='port for webserver to listen on')
    parser.add_argument('--no-browser', action="store_true", default=False, help='Do not automatically open a web browser')
    return parser.parse_args()

def main():
    args = get_cli()
    with open(os.path.join(app.root_path, 'secrets.json')) as secrets_file:
        secret_data = json.load(secrets_file)
    app.secret_key = secret_data['flask_secret_key']
    if not args.no_browser:
        threading.Timer(1, open_browser, args=(args.host, args.port)).start()
    app.run(args.host, args.port, debug=True, use_reloader=False)

if __name__ == '__main__':
    main()
