import os 
if os.name !='nt':
    import ConfigParser as configparser
else:
    import configparser as configparser

import hashlib

class Security(object):    
    def validate_access(self,authorization_):      
       

            config = configparser.ConfigParser()
            config_files = ['config.ini','config_connections.ini']
            config.read(config_files) 
            if authorization_:
                try:
                    hash_request = hashlib.sha256(str(authorization_['username']).encode('utf-8') + str(authorization_['password']).encode('utf-8')).hexdigest()
                    hash_config = config.get("Hash", str(authorization_['username']))
                    if hash_request == hash_config:
                        return True
                except Exception as ex:
                    return False
            return False
 