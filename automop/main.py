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
import subprocess


app = flask.Flask(__name__)

class ShutdownEvent:
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

def get_user_email():
    authorized_session = AuthorizedSession(google.auth.default(['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email'])[0])
    authorized_session.get('https://api.firecloud.org/api/health')
    return id_token.verify_oauth2_token(authorized_session.credentials.id_token, Request(session=authorized_session), clock_skew_in_seconds=10)['email']

@app.route('/')
def index():
    try:
        session['user'] = get_user_email()
    except:
        return render_template('error.html', error_message=Markup(
            'Something went wrong verifying your Google credentials. Make sure you have valid <a href="https://cloud.google.com/docs/authentication/application-default-credentials">Application Default Credentials</a>, ' +
            'usually this done by running <pre>gcloud auth application-default login</pre> in your terminal.'))
    return redirect('/workspaces')

def get_workspace_cost(workspace):
    return fapi.get_storage_cost(workspace[0], workspace[1]).json()['estimate']

@app.route('/get_workspaces')
async def get_workspaces():
    all_workspaces = fapi.list_workspaces().json()
    writer_workspaces = [(workspace['workspace']['namespace'], workspace['workspace']['name']) for workspace in all_workspaces if workspace['accessLevel'] == 'WRITER' or workspace['accessLevel'] == 'OWNER']

    workspace_costs = None

    with ThreadPoolExecutor(max_workers=50) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, get_workspace_cost, workspace) for workspace in writer_workspaces]
        workspace_costs = [cost for cost in await asyncio.gather(*tasks)]
    
    workspaces_json = [{'namespace': writer_workspaces[i][0], 'name': writer_workspaces[i][1], 'cost': workspace_costs[i]} for i in range(len(writer_workspaces))]
    
    return jsonify(sorted(workspaces_json, key=lambda workspace: float(workspace['cost'][1:])))

@app.route('/workspaces')
def workspaces():
    if 'user' not in session:
        return render_template('error.html', error_message=Markup('The user email was not found in the session. Try navigating to the <a href="/">homepage</a>, which will set this variable.'))

    return render_template('workspaces.html', user=session['user'])

def submit_automop_job(workspace, user):
    return 'OK'
    workspace_namespace = workspace[0]
    workspace_name = workspace[1]
    method = {
        'methodRepoMethod': {
            'methodName': 'Automop',
            'methodVersion': 5,
            'methodNamespace': 'DSPMethods_mgatzen',
            'methodUri': 'agora://DSPMethods_mgatzen/Automop/5', 'sourceRepo': 'agora'
            },
        'name': 'Automop',
        'namespace': 'DSPMethods_mgatzen',
        'inputs': {
            'Mop.user': f'"{user}"',
            'Mop.workspace_namespace': f'"{workspace_namespace}"',
            'Mop.workspace_name': f'"{workspace_name}"'
        },
        'outputs': {},
        'methodConfigVersion': 5,
        'deleted': False
    }
    result = fapi.create_workspace_config(workspace_namespace, workspace_name, method)
    if not result.ok:
        return result.json()['message']
    result = fapi.create_submission(workspace_namespace, workspace_name, 'DSPMethods_mgatzen', 'Automop')
    if not result.ok:
        return result.json()['message']
    result = fapi.delete_workspace_config(workspace_namespace, workspace_name, 'DSPMethods_mgatzen', 'Automop')
    if not result.ok:
        return result.json()['message']
    return 'OK'


@app.route('/mop', methods=['POST'])
async def mop():
    workspaces_to_mop = [tuple(workspace_and_checkbox[0].split('/')) for workspace_and_checkbox in request.form.items() if workspace_and_checkbox[1] == 'on']

    if 'user' not in session:
        return render_template('error.html', error_message=Markup('The user email was not found in the session. Try navigating to the <a href="/">homepage</a>, which will set this variable.'))
    
    with ThreadPoolExecutor(max_workers=50) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, submit_automop_job, *(workspace, session['user'])) for workspace in workspaces_to_mop]
        submission_results = [result for result in await asyncio.gather(*tasks)]
    
    mop_results = []
    all_ok = True
    for i in range(len(workspaces_to_mop)):
        if submission_results[i] != 'OK':
            all_ok = False
        mop_results.append({'workspace_namespace': workspaces_to_mop[i][0], 'workspace_name': workspaces_to_mop[i][1], 'status': submission_results[i]})
    return render_template('mop.html', mop_status='All mop jobs submitted successfully.' if all_ok else 'There have been errors submitting the mop jobs:', mop_results=mop_results)

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/stop')
def stop():
    shutdown_server()

@app.route('/stop_and_delete')
def stop_and_delete():
    subprocess.Popen(f'sleep 1 && cd "{os.path.dirname(os.path.realpath(__file__))}/../.." && touch it_worked.txt', shell=True)
    shutdown_server()

def open_browser():
    webbrowser.open_new('http://127.0.0.1:8080')

def main():
    with open(os.path.join(app.root_path, 'secrets.json')) as secrets_file:
        secret_data = json.load(secrets_file)
    app.secret_key = secret_data['flask_secret_key']
    threading.Timer(2, open_browser).start()
    app.run('localhost', 8080, debug=True)

if __name__ == '__main__':
    main()