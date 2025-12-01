from DataStructures.List import array_list as lt

def new_stack():
    """
    Crea una nueva pila vacía.
    """
    return lt.new_list()

def is_empty(stack):
    """
    Verifica si la pila está vacía.
    """
    return lt.is_empty(stack)

def push(stack, item):
    """
    Agrega un elemento a la parte superior de la pila.
    """
    lt.add_first(stack, item)
    return stack

def pop(stack):
    """
    Elimina y retorna el elemento en la parte superior de la pila.
    """
    return lt.remove_first(stack)

def top(stack):
    """
    Retorna el elemento en la parte superior de la pila sin eliminarlo.
    """
    if lt.is_empty(stack):
        raise Exception('EmptyStructureError: stack is empty')
    return lt.get_element(stack, 0)

def size(stack):
    """
    Retorna el número de elementos en la pila.
    """
    return lt.size(stack)