from flask import Flask, jsonify, request, make_response, json
from flask_cors import CORS, cross_origin

from flask_swagger_ui import get_swaggerui_blueprint
from werkzeug.exceptions import BadRequest
from meudesconto import MeuDesconto
from datetime import datetime
from security import Security
from errorobjects import L20notfound
import traceback

import os
if os.name !='nt':
    import ConfigParser as configparser
else:
    import configparser as configparser

app = Flask(__name__)
app.config["APPLICATION_ROOT"] = "/v2"
### swagger specific ###
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "GPA Meu Desconto"
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)

# New Version 2019-02-01 Silas Oliveira
# New Version 2019-06-03 Silas Oliveira - Changes to deploy to HML env
# New Version 2019-11-08 Silas Oliveira - Minor fixes
# New Version 2020-01-29 Andres Alvarez - Refactoring


config = configparser.ConfigParser()
config_files = ['config.ini', 'config_connections.ini']
config.read(config_files)
exception_response = json.loads(config.get('Responses', 'exception_response'))



def get_multiple_audience(auth=True):
    """
        # Queries, calculates and returns the audience, or multiple audiences, received by POST request
        # Parameter auth: Signals if authentication checks ought to be performed
    """
    authOk = True
    response = exception_response

    # Allow skipping authentication checks only if in debug mode
    if auth or not app.debug:
        sec = Security()
        authOk = sec.validate_access(request.authorization)
    else:
        print('\n Authentication checks skipped')


    if authOk:
        try:
            response = []
            audiences = request.json.get('audience')
            
            print("\n *************************")

            for audience in audiences: 
                obj = MeuDesconto()     
                ret_audiencia  = obj.calcular_audiencia_industria(audience)
                resp = make_response(jsonify(ret_audiencia), 200)
                
                new_resp = json.loads(resp.data)
                response.append(new_resp)

            response = make_response(json.dumps(response))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
        except L20notfound as el20:
            now = datetime.now()
            exception_response['Razao'] = el20.message
            traceback.print_exc()
            try:
                f = open("Logchamadas.log", "a")
                message_error = ("\n{0} - Erro: {1} - Parametros : {2}".format(now.strftime("%d/%m/%Y %H:%M:%S"),el20.message,str(audiences)))
                f.write(message_error)
                f.flush()               
                f.close()
            except:
                pass
            ret = jsonify(exception_response)
            return make_response(ret, 200)
        except Exception as e:
            now = datetime.now()
            traceback.print_exc()
            try:
                f = open("Logchamadas.log", "a")
                f.write("\n{0} - Erro: {1} - Parametros : {2}".format(now.strftime("%d/%m/%Y %H:%M:%S"), str(e),str(audiences)))
                f.close()
            except:
                pass
            exception_response['Razao'] = str(e)
            return make_response(jsonify(exception_response), 400)
    else:
        print("\n UNAUTHORIZED ACCESS")
        raise ValueError

    return response


 
@app.route('/getMultipleAudience', methods=['GET', 'POST', 'OPTIONS'])
def get_multiple_audience_NoAuth():
    """
        Endpoint that can be used to make a request without authentication checks
    """
    # Tries to run the request without authentication
    return get_multiple_audience(False)


@app.route('/getMultipleAudienceAuth', methods=['POST'])
@cross_origin(origins=['*'],allow_headers=['Content-Type','Authorization'])
def get_multiple_audience_auth():
    """
        Endpoint that is to be used for calculating audience -- perform authentication checks
    """
    return get_multiple_audience(True)


@app.route('/getReportDataAuth', methods=['POST'])
@cross_origin(origins=['*'],allow_headers=['Content-Type','Authorization'])
def getReportData():
    """
        Endpoint that is to get data  to render reports on platform
    """
    authOk = True
    response = exception_response

    # Allow skipping authentication checks only if in debug mode
    if not app.debug:
        sec = Security()
        authOk = sec.validate_access(request.authorization)
    else:
        print('\n Authentication checks skipped')


    if authOk:
        
        try:
            response = []
            reportfilter = request.json
            
            print("\n *************************")

          
            obj = MeuDesconto()     
            ret_audiencia  = obj.getReportData(reportfilter)
            resp = make_response(jsonify(ret_audiencia), 200)
            
            new_resp = json.loads(resp.data)
            response.append(new_resp)

            response = make_response(json.dumps(response))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
    
        except Exception as e:
            now = datetime.now()
            try:
                f = open("Logchamadas.log", "a")
                f.write("\n{0} - Erro: {1} - Parametros : {2}".format(now.strftime("%d/%m/%Y %H:%M:%S"), str(e),str(reportfilter)))
                f.close()
            except:
                pass
            exception_response['Razao'] = str(e)
            return make_response(jsonify( str(e)),400)
    else:
        print("\n UNAUTHORIZED ACCESS")
        raise ValueError

    return response


@app.route('/getIndicadorAuth', methods=['GET','POST'])
@cross_origin(origins=['*'],allow_headers=['Content-Type','Authorization'])
def getIndicador():
    """
        Endpoint that is to get data  to render reports on platform
    """
    authOk = True
    response = exception_response

    # Allow skipping authentication checks only if in debug mode
    if app.debug:
        sec = Security()
        authOk = sec.validate_access(request.authorization)
    else:
        print('\n Authentication checks skipped')


    if authOk:
        #return True
        try:
            response = []
            reportfilter = request.json
            
            print("\n *************************")

          
            obj = MeuDesconto()     
            ret_audiencia  = obj.getIndicadores()
            resp = make_response(jsonify(ret_audiencia), 200)
            
            new_resp = json.loads(resp.data)
            response.append(new_resp)

            response = make_response(json.dumps(response))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
    
        except Exception as e:
            now = datetime.now()
            try:
                f = open("Logchamadas.log", "a")
                f.write("\n{0} - Erro: {1} - Parametros : {2}".format(now.strftime("%d/%m/%Y %H:%M:%S"), str(e),str(reportfilter)))
                f.close()
            except:
                pass
            exception_response['Razao'] = str(e)
            return make_response(jsonify(exception_response), 400)
    else:
        print("\n UNAUTHORIZED ACCESS")
        raise ValueError

    return response

 

@app.route('/getFiltersAuth', methods=['GET', 'POST', 'OPTIONS'])
@cross_origin(origins=['*'],allow_headers=['Content-Type','Authorization'])
def get_filters_json_auth():
    """
        Returns json with all categories and client DNAs available for building filters in frontend
    """
    try:
        obj = MeuDesconto()    
        filters = obj.get_dataToFilters()        
    except (BadRequest, ValueError):        
        return make_response(jsonify(exception_response), 400)
    except Exception as ex:
        print(ex)

    return make_response(jsonify(filters))


if __name__ == '__main__':
    app.run(debug=True)
