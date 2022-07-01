import json
import requests

from get_table import *

def validate_http(request):
    '''
    Responding and validating any HTTP request
    '''
    request_json = request.get_json()

    if request.args:
        get_tables()
        return f'Data pull complete'
    
    elif request_json:
        get_tables()
        return f'Data pull complete'

    else:
        get_tables()
        return f'Data pull complete'