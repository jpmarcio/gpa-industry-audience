import json
import sys
from testes.datatest import *
from meudesconto import execute_query
from meudesconto import MeuDesconto


obj = MeuDesconto()     



def test_Sql_AudienceLealdade5():
    try:
        filters = data_Sql
        filters[0]['filtros']['lealdadeProduto'] = 5
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
         
        ret = obj.calcular_audiencia_industria(filters[0])          
        
        assert True, 'Erro '
    except Exception as e:          
        assert False,repr(e)

def test_Sql_AudienceLealdade5_UN():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 5
        filters[0]['filtros']['un'] = [{
            "codigo": "01011403034002",
            "descricao": "SHAMPOO BASICO"
            },
            {
            "codigo": "01011403040",
            "descricao": "HIDRATACAO CABELO"
        }]
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade       
        ret = obj.calcular_audiencia_industria(filters[0])          
        
        assert True, 'Erro '
    except Exception as e:          
        assert False,repr(e)

def test_Sql_AudienceLealdade6():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 6
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
              
        ret = obj.calcular_audiencia_industria(filters[0])          
        
        assert True, 'Erro '
    except Exception as e:          
        assert False,repr(e)

def test_Sql_AudienceLealdade7():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 7
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        ret = obj.calcular_audiencia_industria(filters[0])          
        
        assert True, 'Erro '       
    except Exception as e:          
        assert False,repr(e)

def test_Sql_AudienceLealdade10():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 10
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        ret = obj.calcular_audiencia_industria(filters[0])        
    

        assert True, 'Erro '      
    except Exception as e:          
        assert False,repr(e)
   
def test_Sql_AudienceLealdade20():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 20
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
 
        ret = obj.calcular_audiencia_industria(filters[0])  
        assert True, 'Erro '      
    except Exception as e:          
        assert False,repr(e)


def test_Sql_audienceLealdade6_filters():
    try:
        filters = data_Sql_Modalidade.get('audience')
        filters[0]['filtros']['lealdadeProduto'] = 6
        filters[0]['modalidade'] = data_Sql_filterIndustriaIModalidade
        
        ret = obj.calcular_audiencia_industria(filters[0])  
        assert True, 'Erro '      

    except Exception as e:          
        assert False,repr(e)

