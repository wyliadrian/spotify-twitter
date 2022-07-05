#!/usr/bin/env python
# coding: utf-8

import requests
import datetime
from urllib.parse import urlencode

import base64

class SpotifyAPI(object):
    access_token = None
    access_token_expires = datetime.datetime.now()
    client_id = None
    client_secret = None
    token_url = "https://accounts.spotify.com/api/token"
    
    def __init__(self, client_id, client_secret, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        
    def get_client_credentials(self):
        """
        Return a base64 encoded string
        """
        client_id = self.client_id
        client_secret = self.client_secret
        if client_id == None or client_secret == None:
            raise Exception("Must set client_id and client_secret")
        client_creds = f"{client_id}:{client_secret}"
        client_creds_b64 = base64.b64encode(client_creds.encode())
        return client_creds_b64.decode()
    
    def get_token_headers(self):
        client_creds_b64 = self.get_client_credentials()
        return {
            "Authorization": f"Basic {client_creds_b64}"
        }
    
    def get_token_data(self):
        return {
            "grant_type": "client_credentials"
        }
    
    def perform_auth(self):
        """
        Perform Authentication
        """
        token_url = self.token_url
        token_data = self.get_token_data()
        token_headers = self.get_token_headers()
        r = requests.post(token_url, data = token_data, headers = token_headers) 
        if r.status_code not in range(200, 299):
            raise Exception("Could not authenticate client")
        data = r.json()
        now = datetime.datetime.now()
        self.access_token = data['access_token']
        expires_in = data['expires_in'] # seconds
        self.access_token_expires = now + datetime.timedelta(seconds = expires_in)
        return True
    
    def get_access_token(self):
        token = self.access_token
        expires = self.access_token_expires
        now = datetime.datetime.now()
        if expires < now:
            self.perform_auth()
            return self.get_access_token()
        elif token == None:
            self.perform_auth()
            return self.get_access_token()
        return token
    
    def get_resource_header(self):
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        return headers
    
    def get_resource(self, query, version='v1'):
        endpoint = f"https://api.spotify.com/{version}/" + query
        headers = self.get_resource_header()
        r = requests.get(endpoint, headers=headers)
        if r.status_code not in range(200, 299):
            return {}
        return r.json()
         
    def search(self, query):
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        endpoint = "https://api.spotify.com/v1/search"
        lookup_url = f"{endpoint}?{query}"
        r = requests.get(lookup_url, headers = headers)
        if r.status_code not in range(200, 299):
            raise Exception("Unvalid Request")
        return r.json()
