def new_list():
    newlist = {
        'elements': [],
        'size': 0
    }
    return newlist

def get_element(my_list, index):

    return my_list["elements"][index]

def is_present(my_list,element,cmp_function):

    size = my_list["size"]
    if size > 0:
        keyexist = False
        for keypos in range(0,size):
            info = my_list["elements"][keypos]
            if cmp_function(element, info)== 0:
                keyexist = True
                break
        if keyexist:
            return keypos
    return -1

def add_first(my_list, element):
    my_list["elements"].insert(0, element)
    my_list["size"] += 1

def add_last(my_list, element):
    my_list["elements"].append(element)
    my_list["size"] += 1
    
def size(my_list):
    return my_list["size"]

def first_element(my_list):
    if my_list["size"] == 0:
        raise IndexError("list index out of range")
    return my_list["elements"][0]

def is_empty(my_list):
    if my_list["size"]== 0:
        return True
    return False

def last_element(my_list):
    if my_list["size"]== 0:
        raise IndexError("list index out of range")
    
    return my_list["elements"][-1]

def delete_element(my_list,pos):
    
    if pos < 0 or pos >= my_list["size"]:
        raise IndexError("list index out of range")
    
    remove = my_list["elements"].pop(pos)
    
    my_list["size"]-=1
    
    return my_list

def remove_first(my_list):
    if my_list["size"]==0:
        raise IndexError("list index out of range")
    
    remove= my_list["elements"].pop(0)
    my_list["size"]-=1
    
    return remove

def remove_last(my_list):
    if my_list["size"]== 0:
        raise IndexError("list index out of range")
    
    remove = my_list["elements"].pop()
    my_list["size"]-=1
    
    return remove

def insert_element(my_list,element,pos):
    
    my_list["elements"].insert(pos,element)
    
    my_list["size"]+=1
    
    return my_list

def change_info(my_list,pos,new_info):
    
    if pos < 0 or pos >= my_list["size"]:
        raise IndexError("list index out of range")
    
    my_list["elements"][pos]= new_info
    
    return my_list

def exchange(my_list,pos_1,pos_2):
    
    if pos_1 < 0 or pos_1 >= my_list["size"]:
        raise IndexError("list index out of range")
    
    if pos_2 < 0 or pos_2 >= my_list["size"]:
        raise IndexError("list index out of range")
    
    elemento1= my_list["elements"][pos_1]
    elemento2= my_list["elements"][pos_2]
    
    my_list["elements"][pos_1]=elemento2
    my_list["elements"][pos_2]=elemento1
    
    return my_list

def change_element(my_list, position, element): # Para lab6... wtf
    """
    Cambia el elemento en la posición dada (1-based index).
    """
    if position < 0 or position >= my_list["size"]:
        raise IndexError("Posición fuera de rango")

    my_list["elements"][position] = element
    return my_list


def sub_list(my_list,pos_i,num_elements):
    
    if pos_i < 0 or pos_i >= my_list["size"]:
        raise IndexError("list index out of range")
    
    final= min(pos_i+num_elements, my_list["size"])
    
    new_list={"elements":[],"size":0}
    
    for i in range(pos_i,final):
        new_list["elements"].append(my_list["elements"][i])
        new_list["size"]+=1
    
    
    return new_list


# Ordenamiento Iterativo Lab5

def default_sort_criteria(element_1, element_2):
    is_sorted = False
    if element_1 < element_2:
        is_sorted = True
    return is_sorted

def selection_sort(my_list, sort_crit):
    n = my_list["size"]
    for i in range(n - 1):
        min_index = i
        for j in range(i + 1, n):
            if sort_crit(my_list["elements"][j], my_list["elements"][min_index]):
                min_index = j
        if min_index != i:
            my_list["elements"][i], my_list["elements"][min_index] = my_list["elements"][min_index], my_list["elements"][i]
    return my_list


def insertion_sort(my_list, sort_crit):
    n = my_list["size"]
    for i in range(1, n):
        key = my_list["elements"][i]
        j = i - 1
        while j >= 0 and not sort_crit(my_list["elements"][j], key):
            my_list["elements"][j + 1] = my_list["elements"][j]
            j -= 1
        my_list["elements"][j + 1] = key
    return my_list


def shell_sort(my_list, sort_crit):
    n = my_list["size"]
    h = 1
    while h < n // 3:
        h = 3 * h + 1
    while h > 0:
        for i in range(h, n):
            key = my_list["elements"][i]
            j = i
            while j >= h and not sort_crit(my_list["elements"][j - h], key):
                my_list["elements"][j] = my_list["elements"][j - h]
                j -= h
            my_list["elements"][j] = key
        h //= 3
    return my_list

# Ahora los recursivos

def merge_sort(my_list, sort_crit):
    n = size(my_list) # Encontamos el tamaño
    if n <= 1: # Caso base
            return my_list
    mid = n // 2 # Vamos por la mitad
    left_half = sub_list(my_list, 0, mid) # mitad <-
    right_half = sub_list(my_list, mid, n) # mitad ->
    
    left_half = merge_sort(left_half, sort_crit) # Esta es la parte iterativa, recordar pasarle el sort_crit
    right_half = merge_sort(right_half, sort_crit)
    
    return merge(left_half, right_half, sort_crit) # Antes de devolver organizamos
    
def merge(left, right, sort_crit):
    """
    Mezcla dos listas ordenadas en una sola lista ordenada. # < Me lo robe de las presentaciones de Eduardo >:)
    """
    result = new_list()
    i = j = 0 
    
    while i < size(left) and j < size(right): # Mientras tengamos datos iteramos
        if sort_crit(get_element(left, i), get_element(right, j)): # metemos los primeros datos de left y right
            add_last(result, get_element(left, i)) # Este es si gana izquierda
            i += 1
        else: # Este es si gana derecha
            add_last(result, get_element(right, j))
            j += 1
    # Aquí ta salimos y nos quedamos sin listas que organizar (Pero no sin elementos)
    while i < size(left): # Aquí es que quedaron en left y se deben meter todos
        add_last(result, get_element(left, i))
        i += 1
    while j < size(right): # Aquí es que se quedaron en right y se deben meter todolos los que faltaron
        add_last(result, get_element(right, j))
        j += 1
    return result

def quick_sort(my_list, sort_crit):
    n = size(my_list)
    if n <= 1:  # Caso base
        return my_list
    
    pivot = get_element(my_list, n - 1)  # Usamos el último elemento como pivote
    left = new_list()
    right = new_list()

    # Dividir en dos listas
    for i in range(n - 1):  # hasta el penúltimo
        elem = get_element(my_list, i)
        if sort_crit(elem, pivot):  # Si elem < pivote
            add_last(left, elem)
        else:
            add_last(right, elem)
    
    # Ordenar recursivamente
    left = quick_sort(left, sort_crit)
    right = quick_sort(right, sort_crit)

    # Combinar resultados
    result = new_list()
    for i in range(size(left)):
        add_last(result, get_element(left, i))
    add_last(result, pivot)
    for i in range(size(right)):
        add_last(result, get_element(right, i))

    return result
