
import os





class ClassPropertyDescriptor(object):

    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, obj, klass=None):
        if klass is None:
            klass = type(obj)
        return self.fget.__get__(obj, klass)()

    def __set__(self, obj, value):
        if not self.fset:
            raise AttributeError("can't set attribute")
        type_ = type(obj)
        return self.fset.__get__(obj, type_)(value)

    def setter(self, func):
        if not isinstance(func, (classmethod, staticmethod)):
            func = classmethod(func)
        self.fset = func
        return self

def classproperty(func):
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)
    return ClassPropertyDescriptor(func)



# address_state_code - conversion of parameter received from the interface to the corresponding code in database
def state_func(state_code):
    state_dict={
                  'AC':1, 'AL':2,'AM':3, 'AP':4,'BA':5,'CE':6,'DF':7,'ES':8, 'GO':9, 'MA':10,
                  'MG':11, 'MS':12, 'MT':13, 'PA':14, 'PB':15, 'PE':16, 'PI':17, 'PR':18, 'RJ':19, 'RN':20
                , 'RO':21, 'RR':22,'RS':23, 'SC':24, 'SE':25, 'SP':26, 'TO':27
            }
    if state_code in state_dict:    
        return state_dict[state_code]
    else: 
        return -1


def listdecode2str(func, items):
    str_item = ""
    for x in items:
        x = filter_string(x)
        if str_item == "":
            str_item += str(func(x))
        else:
            str_item += "," + str(func(x))
    return "(" + str_item + ")"


def filter_string(value):
    return str(value).replace(" ", "").replace("'", "").replace(";", "")


# gender - conversion of parameter received from the interface to the corresponding code in database
def gender_func(gender_code):
    if gender_code == 'M':
        return 1
    elif gender_code == 'F':
        return 2
    return -1

# price sensibility - conversion of parameter received from the interface to the corresponding code in database
def sens_func(sens_code):
    if sens_code == 'SS':
        return 1
    elif sens_code == 'MM':
        return 2
    elif sens_code == 'LPS':
        return 3
    elif sens_code == 'VPS':
        return 4
    return '-1,1,2,3,4'  # this occurs when 0 is received from the interface

def numlist2str(items):
    str_products = ""
    for x in items:
        x = filter_string(x)
        if str_products == "":
            str_products += str(x)
        else:
            str_products += "," + str(x)
    return "(" + str_products + ")"

def list2str(items):  # the name of this method is self-explained
    y = 0
    str_products = ""
    for x in items:
        x = filter_string(x)
        if y == 0:
            str_products += "'" + str(x) + "'"
        else:
            str_products += ", '" + str(x) + "'"
        y = y + 1
    return str_products








