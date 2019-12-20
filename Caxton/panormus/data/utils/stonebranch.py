import json

import requests

from requests.auth import HTTPBasicAuth

from panormus.config import settings


class StonebranchClient(object):
    '''

    Stonebranch client to communicate with REST api

    '''

    def __init__(self, username, password, url_root=settings.STONEBRANCH_URL_DEV):
        '''

        Create instance with fixed url root and basic authentication header.



        :param str username:

        :param str password:

        :param str url_root:

        '''

        self.url_root = url_root

        self.auth = HTTPBasicAuth(username, password)

        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}


def __check_response(self, response):
    if response.status_code == 200:
        return response

    err_msg = f'{response.status_code}: {response.text}'

    if response.status_code == 400:

        raise StonebranchError.BadRequest(err_msg)

    elif response.status_code == 401:

        raise StonebranchError.BadCredentials(err_msg)

    elif response.status_code == 403:

        raise StonebranchError.PermissionFailure(err_msg)

    elif response.status_code == 404:

        raise StonebranchError.ResourceNotFound(err_msg)

    elif response.status_code == 500:

        raise StonebranchError.BadParameters(err_msg)

    else:

        raise ValueError(err_msg)


def __post(self, address, payload=None):
    payload = payload or {}

    response = requests.post(

        self.url_root + address,

        json=payload,

        auth=self.auth,

        headers=self.headers)

    self.__check_response(response)

    return response


def __get(self, address, payload=None):
    payload = payload or {}

    response = requests.get(

        self.url_root + address,

        params=payload,

        auth=self.auth,

        headers=self.headers)

    self.__check_response(response)

    return response


def __put(self, address, payload=None):
    payload = payload or {}

    response = requests.put(

        self.url_root + address,

        data=json.dumps(payload),

        auth=self.auth,

        headers=self.headers)

    self.__check_response(response)

    return response


def task_search(self, name='*'):
    '''

    Search for tasks using name substring. Wildcards (*) allowed.



    :param str name: name substring

    :rtype: list[str]

    '''

    payload = {'name': name}

    task_list = self.__post('resources/task/list', payload).json()

    return task_list


def task_details(self, task_name='', sys_id=''):
    '''

    Get all details for task by name or sysId.



    :param str task_name: exact task name

    :param str sys_id: exact sysId

    :rtype: dict

    '''

    if task_name:

        params = {'taskname': task_name}

    else:

        params = {'taskid': sys_id}

    task_details = self.__get('resources/task', params).json()

    return task_details


def task_update(self, task_dict):
    '''

    Save changes to an existing task. You can retrieve task details, modify fields, then use this method to update.



    :param dict task_dict: dictionary containing task details

    :return: http response

    '''

    response = self.__put('resources/task', task_dict)

    return response


def task_create(self, task_dict):
    '''

    Save new task. You can retrieve an existing task as a template so long as you \

    set sysId to None and change the name before creating a new task.



    :param dict task_dict: dictionary containing task details

    :return: http response

    '''

    response = self.__post('resources/task', task_dict)

    return response


class StonebranchError(object):
    class BadRequest(Exception):
        pass

    class ResourceNotFound(Exception):
        pass

    class BadCredentials(Exception):
        pass

    class PermissionFailure(Exception):
        pass

    class BadParameters(Exception):
        pass