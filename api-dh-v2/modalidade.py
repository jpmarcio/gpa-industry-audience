from queries import Queries
from utilsobjects import  state_func, listdecode2str, numlist2str,list2str, gender_func, sens_func

import pandas as pd

class Modalidade(object):
    _values = {}
    targetcode = ""
    subgrupo = ""
    level_conversion = "L20"
    #function dictionary to validate parameters according TipoTarget
    
    def __init__(self,modalidade):
        """
            class to store and validate modalidade parameters
        """
        try:
            #super().__init__()
            self._value = modalidade

            if not self._value['tipotarget'] in ('PLU','FAMILIA','SUB-GRUPO','GRUPO','SUB-CATEGORIA','CATEGORIA'):
                    raise Exception('Tipo Target invlido')

                

            depart = modalidade.get('departamento')
            categoria = modalidade.get('categoria')
            self.level_conversion = 'L40'
            subcategoria = ""
            if self._value['tipotarget'] in ('SUB-CATEGORIA','SUB-GRUPO','GRUPO','FAMILIA','PLU'):
                subcategoria = modalidade.get('subcategoria')
                self.level_conversion = 'L30'
            grupo =  ""
            if self._value['tipotarget'] in ('GRUPO','SUB-GRUPO','FAMILIA','PLU'):
                grupo =  modalidade.get('grupo')
                self.level_conversion = 'L20'
            subgrupo = ""
            if "subgrupo" in modalidade.keys():
                subgrupo = modalidade.get('subgrupo')

            code = (depart,categoria,subcategoria,grupo)
            self.targetcode = "".join(code)
            self.subgrupo = self.targetcode +  subgrupo
            
            validatefunctions= {
                'PLU': validatePlu,
                'FAMILIA':ValidateFamilia,
                'SUB-GRUPO':ValidateSubgrupo,
                'GRUPO':ValidateGrupo,
                'SUB-CATEGORIA':ValidateSubcategoria,
                'CATEGORIA':ValidateCategoria
            }
            
            # Get the function from switcher dictionary
            func = validatefunctions.get(self._value['tipotarget'], lambda: "Invalid target")
            # Execute the function

            func(self._value)
        except Exception as ex:
            print(str(ex.message))
            raise ex
    
    def getinfoPLU(self):

        modalidade = self._value        


        strquery = Queries.getProductInfo()
        strquery = strquery.format(produto)
        subcategoria = ""
        grupo =  ""
        subgrupo = ""
        subgrupo = ""


       
    def getl20_30(self,vendor):

        getfunctions= {
            'PLU': getl20_30Plu,
            'FAMILIA':getl20_30Familia,
            'SUB-GRUPO':getl20_30subgrupo,
            'GRUPO':getl20_30grupo,
            'SUB-CATEGORIA':getl20_30subcategory,
            'CATEGORIA':getl20_30category
        }
        # Get the function from switcher dictionary
        func = getfunctions.get(self._value['tipotarget'], lambda: "Invalid target")
        # Execute the function
        vendor_sql = ""
        if vendor!="":
            vendor_sql += " AND  ps.supplier_code = '{}' ".format(vendor)

        query = func(self,vendor_sql)

        return query 

def getl20_30Plu(modalidade,vendor):
    """
        Create a temporary table with the products of this target by a PLU list
        return L20 and l30 of the product
    """
    
    query = Queries().ProductStructure()
    str_prod_codes = list2str(modalidade._value.get("produto"))
  
    query = query.format(wheres=" prod_code in ({prods})".format(prods=str_prod_codes),vendor=vendor)
    
   
    return query
    
def getl20_30Familia(modalidade,vendor):
    """
        Create a temporary table with the products of this target by a PLU and Familia
        return L20 and l30 of the product
    """
  
    query = Queries().ProductStructureFamily()
    
    
    family_code = modalidade._value.get("familia")
    #subgrupo = modalidade._value.get("subgrupo")

    #modalidade.subgrupo

    query = query.format(subgrupo=modalidade.subgrupo,family=family_code,vendor=vendor)

    return query

def getl20_30subgrupo(modalidade,vendor):
    """
        Create a temporary table with the products of this target by a subgroup
        return L20 and l30 of the product
    """
 

    query = Queries().ProductStructure()
    depart = modalidade._value.get('departamento')
    categoria = modalidade._value.get('categoria')
    subcategoria = modalidade._value.get('subcategoria')
    grupo =  modalidade._value.get('grupo')
    subgrupo =  modalidade._value.get('subgrupo')
    
    code = (depart,categoria,subcategoria,grupo,subgrupo)
    str_codes = "".join(code)
    query = Queries().ProductStructure()

    query = query.format(wheres=" prod_hier_l10_code in ('{subgrupo}')".format(subgrupo=str_codes),vendor=vendor)
    
    return query

def getl20_30grupo(modalidade,vendor):
    """
        Create a temporary table with the products of this target by a group
        return L20 and l30 of the product
    """
    

    query = Queries().ProductStructure()
    depart = modalidade._value.get('departamento')
    categoria = modalidade._value.get('categoria')
    subcategoria = modalidade._value.get('subcategoria')
    grupo =  modalidade._value.get('grupo')
    
    code = (depart,categoria,subcategoria,grupo)
    str_codes = "".join(code)
    query = query.format(wheres=" prod_hier_l20_code in ('{grupo}')".format(grupo=str_codes),vendor=vendor)
    
    return query
    
def getl20_30subcategory(modalidade,vendor):
    """
        Create a temporary table with the products of this target by a subcategory
        return L20 and l30 of the product
    """
 

    query = Queries().ProductStructure()
    depart = modalidade._value.get('departamento')
    categoria = modalidade._value.get('categoria')
    subcategoria = modalidade._value.get('subcategoria')
    
    
    code = (depart,categoria,subcategoria)
    str_codes = "".join(code)
    query = query.format(wheres="prod_hier_l30_code in ('{grupo}')".format(grupo=str_codes),vendor=vendor)
    
    return query


def getl20_30category(modalidade,vendor):
    """
        Create a temporary table with the products of this target by a category
        return L20 and l30 of the product
    """
 
    query = Queries().ProductStructure()
    depart = modalidade._value.get('departamento')
    categoria = modalidade._value.get('categoria')
    
    
    
    code = (depart,categoria)
    str_codes = "".join(code)
    query = query.format(wheres="prod_hier_l40_code in ('{grupo}')".format(grupo=str_codes),vendor=vendor)
    
    return query


def validatePlu(modalidade):
    """
        Validate parameters for PLUs
    """

    
    if not 'produto' in modalidade.keys() or not type(modalidade['produto']) is list:
        raise Exception('Produto deve ser preenchido ou ser uma lista')

    if not "subgrupo" in modalidade.keys():      
        raise Exception('Subgrupo obrigatorio para este target')
    elif len(modalidade['subgrupo'])==0:
        raise Exception('Subgrupo obrigatorio para este target')

    if not "grupo" in modalidade.keys():      
        raise Exception('Grupo obrigatorio para este target')
    elif len(modalidade['grupo'])==0:
        raise Exception('Grupo obrigatorio para este target')

    if not "subcategoria" in modalidade.keys():      
        raise Exception('Subcategoria obrigatoria para este target')
    elif len(modalidade['subcategoria'])==0:
        raise Exception('subcategoria obrigatoria para este target')

    if not "categoria" in modalidade.keys():      
        raise Exception('Categoria obrigatoria para este target')
    elif len(modalidade['categoria'])==0:
        raise Exception('Categoria obrigatoria para este target')

    if not "departamento" in modalidade.keys():      
        raise Exception('Departamento obrigatorio para este target')
    elif len(modalidade['departamento'])==0:
        raise Exception('Departamento obrigatorio para este target')

def ValidateSubgrupo(modalidade):
    """
        Validate parameters for subgrupo
    """

    if not "subgrupo" in modalidade.keys():      
        raise Exception('Subgrupo obrigatorio para este target')
    elif len(modalidade['subgrupo'])==0:
        raise Exception('Subgrupo obrigatorio para este target')

    if not "grupo" in modalidade.keys():      
        raise Exception('Grupo obrigatorio para este target')
    elif len(modalidade['grupo'])==0:
        raise Exception('Grupo obrigatorio para este target')

    if not "subcategoria" in modalidade.keys():      
        raise Exception('Subcategoria obrigatoria para este target')
    elif len(modalidade['subcategoria'])==0:
        raise Exception('subcategoria obrigatoria para este target')

    if not "categoria" in modalidade.keys():      
        raise Exception('Categoria obrigatoria para este target')
    elif len(modalidade['categoria'])==0:
        raise Exception('Categoria obrigatoria para este target')

    if not "departamento" in modalidade.keys():      
        raise Exception('Departamento obrigatorio para este target')
    elif len(modalidade['departamento'])==0:
        raise Exception('Departamento obrigatorio para este target')


def ValidateGrupo(modalidade):
    """
        Validate parameters for group
    """

    if not "grupo" in modalidade.keys():      
        raise Exception('Grupo obrigatorio para este target')
    elif len(modalidade['grupo'])==0:
        raise Exception('Grupo obrigatorio para este target')

    if not "subcategoria" in modalidade.keys():      
        raise Exception('Subcategoria obrigatoria para este target')
    elif len(modalidade['subcategoria'])==0:
        raise Exception('subcategoria obrigatoria para este target')

    if not "categoria" in modalidade.keys():      
        raise Exception('Categoria obrigatoria para este target')
    elif len(modalidade['categoria'])==0:
        raise Exception('Categoria obrigatoria para este target')

    if not "departamento" in modalidade.keys():      
        raise Exception('Departamento obrigatorio para este target')
    elif len(modalidade['departamento'])==0:
        raise Exception('Departamento obrigatorio para este target')

def ValidateSubcategoria(modalidade):
    """
        Validate parameters for subcategory
    """


    if not "subcategoria" in modalidade.keys():      
        raise Exception('Subcategoria obrigatoria para este target')
    elif len(modalidade['subcategoria'])==0:
        raise Exception('subcategoria obrigatoria para este target')

    if not "categoria" in modalidade.keys():      
        raise Exception('Categoria obrigatoria para este target')
    elif len(modalidade['categoria'])==0:
        raise Exception('Categoria obrigatoria para este target')

    if not "departamento" in modalidade.keys():      
        raise Exception('Departamento obrigatorio para este target')
    elif len(modalidade['departamento'])==0:
        raise Exception('Departamento obrigatorio para este target')

def ValidateCategoria(modalidade):
    """
        Validate parameters for category
    """

    if not "categoria" in modalidade.keys():      
        raise Exception('Categoria obrigatoria para este target')
    elif len(modalidade['categoria'])==0:
        raise Exception('Categoria obrigatoria para este target')

    if not "departamento" in modalidade.keys():      
        raise Exception('Departamento obrigatorio para este target')
    elif len(modalidade['departamento'])==0:
        raise Exception('Departamento obrigatorio para este target')


def ValidateFamilia(modalidade):
    """
        Validate parameters for FAMILIA
    """
    
    if not 'produto' in modalidade.keys() or not type(modalidade['produto']) is list:
        raise Exception('Produto deve ser preenchido ou ser uma lista')

    if not 'familia' in modalidade.keys() :
        raise Exception('Familia deve ser preenchido ou ser uma lista')
    elif len(modalidade.get('familia'))==0:
        raise Exception('Familia deve ser preenchido ou ser uma lista')

    if not "subgrupo" in modalidade.keys():      
        raise Exception('Subgrupo obrigatorio para este target')
    elif len(modalidade['subgrupo'])==0:
        raise Exception('Subgrupo obrigatorio para este target')

    if not "grupo" in modalidade.keys():      
        raise Exception('Grupo obrigatorio para este target')
    elif len(modalidade['grupo'])==0:
        raise Exception('Grupo obrigatorio para este target')

    if not "subcategoria" in modalidade.keys():      
        raise Exception('Subcategoria obrigatoria para este target')
    elif len(modalidade['subcategoria'])==0:
        raise Exception('subcategoria obrigatoria para este target')

    if not "categoria" in modalidade.keys():      
        raise Exception('Categoria obrigatoria para este target')
    elif len(modalidade['categoria'])==0:
        raise Exception('Categoria obrigatoria para este target')

    if not "departamento" in modalidade.keys():      
        raise Exception('Departamento obrigatorio para este target')
    elif len(modalidade['departamento'])==0:
        raise Exception('Departamento obrigatorio para este target')
  
