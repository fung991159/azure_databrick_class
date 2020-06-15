import requests
import json
import base64


class DataBrick():

    def __init__(self, pd=False):
        if pd: # production env credential
            self.token = 'foo'
            self.cluster_id = 'foo'
            self.is_pd = True
        else: # dev env credential
            self.token = 'bar'
            self.cluster_id = 'bar'
            self.is_pd = False
        self.instance_name = 'southeastasia.azuredatabricks.net'
        self.base_url = f'https://{self.instance_name}/api/2.0/'

    def _gen_r(self, action, body, http_method):
        """databrick request generator"""
        if http_method.lower() == 'post':
            response = requests.post(
                self.base_url + action,
                headers={'Authorization': f'Bearer {self.token}'},
                json=body
            )
        elif http_method.lower() == 'get':
            response = requests.get(
                self.base_url + action,
                headers={'Authorization': f'Bearer {self.token}'},
                json=body
            )

        return response.json()
 
    def ws_list(self, ws_path, raw_output=False):
        """Recursively list all file in a workspace, return list of paths"""
        if ws_path[0] != '/':
            ws_path = '/' + ws_path

        output = []
        next_dir = []
        while True:
            body = {"path": f"{ws_path}"}
            r = self._gen_r('workspace/list', body, 'get')
            r = json.dumps(r, indent=2)
            d = json.loads(r)

            try:
                for result in d['objects']:
                    if result['object_type'] == 'DIRECTORY':
                        next_dir.append(result['path'])
                    else:
                        output.append(result['path'])
            except KeyError:  # blank folder will have empty dict
                pass

            if next_dir:
                ws_path = next_dir.pop(0)
            else:
                break

        for o in output: 
            print(o)
        return output

    def ws_mkdir(self, ws_path):
        if ws_path[0] != '/':
            ws_path = '/' + ws_path

        body = {"path": f"{ws_path}"}
        r = self._gen_r('workspace/mkdirs', body, 'post')
        print(r)
        print(f'path {ws_path} created')
    
    def ws_export(self, ws_path, format='DBC', as_file=True):
        """
        Args:
            format (str): can be <SOURCE, HTML, JUPYTER, DBC>, dir must be DBC
            as_file (bol): download as file
        Return:
            content as base64-encoded bytes
        """
        if ws_path[0] != '/':
            ws_path = '/' + ws_path

        body = {
            "path": f"{ws_path}"
            , "format": f"{format}"
        }
        r = self._gen_r('workspace/export', body, 'get')
        if as_file:
            fn = ws_path.split('/')[-1] + '.' + r['file_type']
            data = base64.b64decode(r['content'])
            with open('/home/fung/Downloads/' + fn, 'wb') as f:
                f.write(data)

        return r['content']

    def ws_import(self, ws_path, base64_content, format='DBC', overwrite=False,
                  print_output=True):
        if ws_path[0] != '/':
            ws_path = '/' + ws_path

        body = {
            "path": f"{ws_path}",
            "format": f"{format}",
            "content": f"{base64_content}",
            "overwrite": overwrite
        }
        r = self._gen_r('workspace/import', body, 'post')
        try:
            print(r['error_code'])
        except KeyError:
            if print_output:
                print(f'{ws_path} imported')

    def ws_migrate(self, ws_path):
        """migrate nb/dir between pd and np"""
        byte_content = self.ws_export(ws_path, as_file=False)
        if self.is_pd:
            pd_db = DataBrick()
            pd_db.ws_import(ws_path, byte_content, overwrite=False,
                            print_output=False)
        else:
            db = DataBrick(True)
            db.ws_import(ws_path, byte_content, overwrite=False,
                         print_output=False)
        print(f'{ws_path} migrated')

    def run_nb(self, ws_path, paras=None, run_name=None):
        """run a notebook, return run_id
        Args:
        ws_path (str)
        paras (dict): key=para name, value=para value

        Returns:
        run_id
        """
      
        if ws_path[0] != '/':
            ws_path = '/' + ws_path
        # turn v1 input params in to string 
        for k, v in paras.items():
            if isinstance(v, dict):
                paras[k] = str(v)

        nb_name = ws_path.split('/')[-1]
        if run_name is None:
            run_name = f'fung_adhoc_run_{nb_name}'
        body = {
            "existing_cluster_id": f"{self.cluster_id}",
            "notebook_task": {
                "notebook_path": f"{ws_path}",
                "base_parameters": paras
                },
            "run_name": f"{run_name}"
        }
        r = self._gen_r('jobs/runs/submit', body, 'post')

        print(f'running {nb_name}', f"run_id: {r['run_id']}")
        return r['run_id']

    def run_list(self, run_id=None):
        """get run status from run_id"""
        # translate run_id to job_id first
        job_body = {
            "run_id": run_id
            # "existing_cluster_id": f"{self.cluster_id}",
            # "notebook_task": {"notebook_path": f"{ws_path}"},
            # "run_name": f"{run_name}"
        }
        r_job = self._gen_r('jobs/runs/get', job_body, 'get')
        job_id = r_job['job_id']

        body = {
            "job_id": job_id
            # "existing_cluster_id": f"{self.cluster_id}",
            # "notebook_task": {"notebook_path": f"{ws_path}"},
            # "run_name": f"{run_name}"
        }
        r = self._gen_r('jobs/runs/list', body, 'get')
        r = json.dumps(r, indent=2)
        print(r)