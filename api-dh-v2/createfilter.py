from filterIndustria import FilterIndustria
from filterAlocacao import FilterAlocacao

class GetFilter(object):
    def __init__(self,plataforma):
        #super().__init__()
        #super(GetFilter, self).__init__()
        self._Plataforma = plataforma
    
    _Plataforma = ""

    def get(self,filters):
        fil = _get_filter(self._Plataforma)
        return fil(filters)


def _get_filter(plataforma):
    if plataforma  == 'INDUSTRIA':
        return _getFilterIndustria
    elif plataforma  == 'PESQUISA':
        return _getFilterPesquisa
    elif plataforma  == 'ALOCACAO':
        return _getFilterAlocacao
    else:
        raise ValueError(plataforma)


def _getFilterIndustria(filters):
    payload = filters
    obj = FilterIndustria()
    obj.filter = filters     
    return obj

def _getFilterAlocacao(filters,plataforma):
    payload = filters
    obj = FilterAlocacao()
    obj.filter = filters
   
    
 
    return obj

def _getFilterPesquisa(filters,plataforma):
    obj = FilterIndustria()
    obj.filter = filters
  
    return obj