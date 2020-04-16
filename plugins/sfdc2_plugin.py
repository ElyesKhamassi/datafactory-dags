# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink

# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
from datetime import datetime
import time


class Payload(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)
        

class RespAsync(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)

class RespAsyncRetry(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)

# Will show up under airflow.operators.test_plugin.PluginOperator
class LongHttpJobOperator(BaseOperator):

    template_fields = ['endpoint', 'data', 'headers', ]

    template_ext = ()

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 endpointAsync,
                 method='POST',
                 data=None,
                 headers=None,
                 response_check=None,
                 response_check_callback=None,
                 extra_options=None,
                 xcom_push=False,
                 http_conn_id='http_default',
                 log_response=False,
                 *args, **kwargs):
        super(LongHttpJobOperator, self).__init__(*args, **kwargs)
        self.log.info("Init with data : " + data)
        
        p = Payload(data)
        self.jobId = p.xID
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.endpointAsync = endpointAsync
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.response_check_callback = response_check_callback
        self.extra_options = extra_options or {}
        self.xcom_push_flag = xcom_push
        self.log_response = log_response

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.log.info("data :" + self.jobId)
        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint,
                              self.data,
                              self.headers,
                              self.extra_options)
        self.log.info("Launch job call status : " + str(response.status_code))

        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False with code : " + str(response.status_code) + " and message : " + response.text) 
        
        rJson = RespAsync(response.text)
        eta = rJson.eta
        retryInterval = rJson.retryInterval
        job = rJson.jobId
        
       # timestamp = int(time.time())
        #timestamp = datetime.timestamp(now)
        httpCallback = HttpHook('GET', http_conn_id=self.http_conn_id)

        time.sleep(eta)
        while True:
            respCallback = httpCallback.run(self.endpointAsync+"/"+job,
                              None,
                              self.headers,
                              self.extra_options)
            
            if respCallback.status_code != 200:
                raise AirflowException("Error while calling callback method check returned False with code : " + str(respCallback.status_code) + " and message : " + respCallback.text)
            
            retryResp = RespAsyncRetry(respCallback.text)
            self.log.info("Retry call with status : " + retryResp.status)

            if retryResp.status == "PENDING":
                self.log.info("waiting ...")
                time.sleep(retryInterval)
                continue
            else:
                if self.response_check_callback:
                    if not self.response_check_callback(retryResp):
                        raise AirflowException("Response check returned False with code : " + str(retryResp.status) + " message : " + retryResp.message)
        
            if self.xcom_push_flag:
                return respCallback.text
            break


# Defining the plugin class
class Sfdc2Plugin(AirflowPlugin):
    name = "Sfdc2Plugin"
    operators = [LongHttpJobOperator]
