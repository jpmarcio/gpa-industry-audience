import json
import sys
from createfilter import GetFilter
from testes.datatest import *

from meudesconto import MeuDesconto,execute_query


def test_padrao():
    try:
        obj = MeuDesconto()    
        filters = obj.get_dataToFilters()    
        assert True
    except Exception as e:          
        assert False,repr(e)