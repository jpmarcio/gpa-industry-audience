import json
import sys
from createfilter import GetFilter
from testes.datatest import *
from meudesconto import MeuDesconto
import pandas as pd


obj = MeuDesconto()     

def test_padrao():
    try:
        obj = MeuDesconto()   

        filterjson = json.loads('{"vendor_class_code": "5498","vendor_code": "","regioes": ["00040000","00040300"],"bandeira": "PA"}')
       
        filters = obj.getReportData(filterjson)    
        
 
        assert True
    except Exception as e:          
        assert False,repr(e)


def test_indicadores():
    try:
        obj = MeuDesconto()   

       
        filters = obj.getIndicadores()    
        
 
        assert True
    except Exception as e:          
        assert False,repr(e)


