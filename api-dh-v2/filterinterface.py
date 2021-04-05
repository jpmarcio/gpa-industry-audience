from utilsobjects import classproperty, state_func, listdecode2str, numlist2str,list2str, gender_func, sens_func
from queries import Queries
from aux_data import get_column_from_dna_id
from modalidade import Modalidade
import pandas as pd 
import traceback
class FilterInterface(object):
    def __init__(self):
        #super().__init__()
        pass
    
    @classproperty
    def filter(self):
        pass

    @filter.setter
    def filter(self, value):
        pass
    
    @classproperty
    def TipoPlataforma(self):
        pass


    @TipoPlataforma.setter
    def TipoPlataforma(self, value):
        pass

    @classproperty
    def l30(self):
        pass


    @l30.setter
    def l30(self, value):
        self._l30 = value


    @classproperty
    def prod_codes(self):
        pass

    @prod_codes.setter
    def prod_codes(self, value):
        pass

    @classproperty
    def modalidade(self):
        pass

    @modalidade.setter
    def modalidade(self, value):
        pass
    
    def getSqlProductTemporaryTable(self):
        pass
    

    

    def get_sql_filters(self):
        pass

    def get_average_price(self):
       pass
    
    

