# Databricks notebook source
import requests
import json

class DBC_REST_API (object):
    """
    This class acts as a wrapper for making calls to Databricks REST API
    """
    
    def __init__(self, user, password, host):
        """
        """

        if not user or not password or not host:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host     = host
        self.user     = user
        self.password = password

    def list_clusters(self):
        """
        Desc: Returns information about all clusters which are currently active or which were terminated within the past hour.
        URL:  GET /2.0/clusters/list
        :param run_id:
        :return:
        """
        api_call ='/api/2.0/clusters/list'
        payload={}
        req = self.__get_request(api_call, payload)
        return req
    
    def print_clusters(self, response):
        if response.status_code == 200:
          for cluster in response.json()['clusters']:
            if cluster['state'] != "TERMINATED":
              print "Name: " + cluster['cluster_name']
        else:
          raise ValueError('Error in REST API Class')

    def create_cluster(self, cluster_specs):
        """
        Desc: Create a cluster -
        URL:  POST /2.0/clusters/create
        :param cluster_specs: Specifications of cluster to create
        :return: Cluster ID of newly created cluster
        """

        api_call='/api/2.0/clusters/create'
        payload = cluster_specs
        #payload["cluster_id"]=cluster_id

        req = self.__post_request(api_call, payload)
        return req
       
    def __post_request(self, api_call, payload):
        """

        :param api_call:
        :param payload:
        :return:
        """


        url = self.host + api_call

        # Convert dict to json
        payload_json=json.dumps(payload, ensure_ascii=False)

        req = requests.post(url, auth=(self.user,self.password), data=payload_json)
        return req


    def __get_request(self, api_call, payload):
        """

        :param api_call:
        :param payload:
        :return:
        """

        url = self.host + api_call

        # Convert dict to json
        payload_json=json.dumps(payload, ensure_ascii=False)

        req = requests.get(url, auth=(self.user,self.password), data=payload_json)

        return req
