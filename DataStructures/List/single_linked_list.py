def new_list():
    newlist = {
        "first": None,
        "last": None,
        "size": 0,
    }
    
    return newlist

def get_element(my_list, pos):
    searchpos = 0
    node = my_list["first"]
    while searchpos < pos:
        node = node["next"]
        searchpos += 1
    return node["info"]

def is_present(my_list, element, cmp_function):
    is_in_array = False
    temp = my_list["first"]
    count = 0
    while not is_in_array and temp is not None:
        if cmp_function(element, temp["info"]) == 0:
            is_in_array = True
        else:
            temp = temp["next"]
            count += 1
    if not is_in_array:
        count = -1
    return count

def add_first(my_list, element): # Vemos que es O(1)
    new_node = {
        "info": element,
        "next": my_list["first"]
    }
    my_list["first"] = new_node
    if my_list["last"] is None:  
        my_list["last"] = new_node
    my_list["size"] += 1
    return my_list
    
    
def add_last(my_list, element):
    # Creo nuevo nodo
    new_node = {
        "info": element,
        "next": None
    }
    # Si la lista no está vacía, el nuevo nodo se convierte en el siguiente del último nodo
    if my_list["last"] is not None:
        my_list["last"]["next"] = new_node
    # Actualizamos el último nodo
    my_list["last"] = new_node
    # Si la lista estaba vacía, el nuevo nodo es también el primero
    if my_list["first"] is None:
        my_list["first"] = new_node
    # Sumamos el nuevo nodo al tamaño de la lista
    my_list["size"] += 1
    return my_list

def size(my_list):
    return my_list["size"]

def first_element(my_list):
    return my_list["first"]["info"]

def is_empty(my_list):
    # Aquí la idea es que si ya estamos manteniendo el size, might as well use it no?
    # Y para hacerlo aún más elegante es cuestion de hacer que el return sea
    # el resultado de un checkeo de validez, así retorna true o false sin
    # agregar más lógica
    return my_list["size"] == 0


def last_element(my_list):
    # En la documentación nos dicen básicamente que inclyamos esto
    # y es super bueno porque ya hice is_empy.
    if is_empty(my_list):
        raise Exception('IndexError: list index out of range')
    # Ahora accedemos a la referencia del último nodo, mediante my_list["last"]
    # con esto ya podemos acceder a su información como si tuvieramos el diccionarío,
    # ¡Se siente un tanto ilegal pero así funciona!
    return my_list["last"]["info"]

# Ahora para implementar delete_element ya sabemos que la complejidad va a se
# O(n) a menos que se trate del primer elemento, en tal caso es O(1)

def delete_element(my_list, pos):
# De nuevo de los docs vemos que debemos sacar este warning en caso de tener
# una posición no valida
    if pos < 0 or pos >= size(my_list):
        raise Exception('IndexError: list index out of range')
# Para eliminar en primer posición sabemos que no necesitamos más que O(1)
    if pos == 0:
        # Actualizamos el First
        my_list["first"] = my_list["first"]["next"]
        my_list["size"] -= 1
# Ahora debemos pensar en el caso hipotetico en el que 
# este segundo elementos que paso a ser el primero, sea también el último
        if my_list["size"] == 0:
            my_list["last"] = None
            my_list["first"] = None
            # my_list["size"] -= 1 OJO AQUÍ NO DEBE RESTARLE
# Ahora si toca cosita O(n) :c 
    else: 
        # Para eliminar en cualquier otra posición, necesitamos recorrer la lista
        current = my_list["first"] # Entro al primer nodo
        for _ in range(pos - 1):
            # Recorro hasta antes de la lista que voy a
            current = current["next"] 
        current["next"] = current["next"]["next"] # Actualizo el link de la anterior, saltandome la que quiero borrar 
        # Finalmente hay que considerar que ahora current["next"] podría ser None
        if current["next"] is None:
            my_list["last"] = current
        my_list["size"] -= 1
    return my_list


# Yo no se si esto es buena practica JAJAJA
def remove_first(my_list):
    if is_empty(my_list):
        raise Exception('IndexError: list index out of range')
    valor = my_list["first"]["info"]
    delete_element(my_list,0)
    return valor

def remove_last(my_list):
    if is_empty(my_list):
        raise Exception('IndexError: list index out of range')
    valor = my_list["last"]["info"]
    delete_element(my_list, my_list['size'] - 1)
    return valor

def insert_element(my_list, pos, element):
 
    if pos < 0 or pos > size(my_list)+1:
        raise Exception('IndexError: list index out of range')
    if pos == 0:
        return add_first(my_list, element)
    if pos == my_list['size']+1:
        return add_last(my_list, element)

    current = my_list["first"] # Entro al primer nodo
    for _ in range(pos - 1):
        # Recorro hasta antes de la lista que voy a la posición antes de agregar el nodo
        current = current["next"]
    new_node = {
        "info": element,
        "next": current["next"],
    }  
    current["next"] = new_node  
    my_list["size"] += 1
    
    return my_list

def change_info(my_list, pos, new_info):
    searchpos = 0
    node = my_list["first"]
    while searchpos < pos:
        node = node["next"]
        searchpos += 1
    node["info"] = new_info
    return my_list

def exchange(my_list, pos1, pos2):
    if pos1 < 0 or pos1 >= size(my_list):
        raise Exception('IndexError: list index out of range')
    if pos2 < 0 or pos2 >= size(my_list):
        raise Exception('IndexError: list index out of range')
    info1 = get_element(my_list, pos1)
    info2 = get_element(my_list, pos2)
    change_info(my_list, pos2, info1)
    change_info(my_list, pos1, info2)
    return my_list

def sub_list(my_list, pos, num_elements):
    # Validaciones de índice
    if pos < 0 or pos >= size(my_list):
        raise Exception("IndexError: list index out of range")
    if num_elements < 0 or pos + num_elements > size(my_list):
        raise Exception("IndexError: list index out of range")

    # Crear nueva lista vacia
    newlist = new_list()

    # Encontrar el nodo inicial desde donde copiar
    node = my_list["first"]
    for _ in range(pos):
        node = node["next"]

    # Copiar los siguientes num_elements nodos a la nueva lista
    for _ in range(num_elements):
        add_last(newlist, node["info"])
        node = node["next"]

    return newlist

# Ordenamiento Iterativo Lab5

def default_sort_criteria(element_1, element_2):
    is_sorted = False
    if element_1 < element_2:
        is_sorted = True
    return is_sorted

def selection_sort(my_list, sort_crit):
    n = size(my_list)
    for i in range(n - 1):
        min_index = i
        for j in range(i + 1, n):
            if sort_crit(get_element(my_list, j), get_element(my_list, min_index)):
                min_index = j
        if min_index != i:
            exchange(my_list, i, min_index)
    return my_list


def insertion_sort(my_list, sort_crit):
    n = size(my_list)
    for i in range(1, n):
        key = get_element(my_list, i)
        j = i - 1
        while j >= 0 and not sort_crit(get_element(my_list, j), key):
            change_info(my_list, j + 1, get_element(my_list, j))
            j -= 1
        change_info(my_list, j + 1, key)
    return my_list


def shell_sort(my_list, sort_crit):
    n = size(my_list)
    h = 1
    while h < n // 3:
        h = 3 * h + 1
    while h > 0:
        for i in range(h, n):
            key = get_element(my_list, i)
            j = i
            while j >= h and not sort_crit(get_element(my_list, j - h), key):
                change_info(my_list, j, get_element(my_list, j - h))
                j -= h
            change_info(my_list, j, key)
        h //= 3
    return my_list

# Ahora los recursivos

def merge_sort(my_list, sort_crit):
    n = size(my_list)
    if n <= 1:
            return my_list
    mid = n // 2
    left_half = sub_list(my_list, 0, mid)
    # right_half = sub_list(my_list, mid, n) Esto ya no funciona por la forma en la que se implemento sub_list para single linked list
    right_half = sub_list(my_list, mid, n-mid)

    
    left_half = merge_sort(left_half, sort_crit)
    right_half = merge_sort(right_half, sort_crit)
    
    return merge(left_half, right_half, sort_crit)
    
def merge(left, right, sort_crit):
    """
    Mezcla dos listas ordenadas en una sola lista ordenada.
    """
    result = new_list()
    i = j = 0
    
    while i < size(left) and j < size(right):
        if sort_crit(get_element(left, i), get_element(right, j)):
            add_last(result, get_element(left, i))
            i += 1
        else:
            add_last(result, get_element(right, j))
            j += 1
    
    while i < size(left):
        add_last(result, get_element(left, i))
        i += 1
    while j < size(right):
        add_last(result, get_element(right, j))
        j += 1
    return result

def quick_sort(my_list, sort_crit):
    pass

def quick_sort(my_list, sort_crit):
    if size(my_list) <= 1:  # Caso base
        return my_list

    pivot = get_element(my_list, size(my_list) - 1)  # Último nodo como pivote
    left = new_list()
    right = new_list()

    current = my_list["first"]
    count = 0
    while count < size(my_list) - 1:  # hasta el penúltimo
        elem = current["info"]
        if sort_crit(elem, pivot):  # elem < pivote
            add_last(left, elem)
        else:
            add_last(right, elem)
        current = current["next"]
        count += 1

    # Recursión
    left = quick_sort(left, sort_crit)
    right = quick_sort(right, sort_crit)

    # Combinar resultados
    result = new_list()
    # Agregar todos los de la izquierda
    current = left["first"]
    while current:
        add_last(result, current["info"])
        current = current["next"]
    # Agregar el pivote
    add_last(result, pivot)
    # Agregar todos los de la derecha
    current = right["first"]
    while current:
        add_last(result, current["info"])
        current = current["next"]

    return result

def remove_at(lst, index):
    """
    Elimina y retorna el elemento en la posición `index` (0-based)
    de la lista enlazada simple usada en EDA.
    """
    if index < 0 or index >= lst["size"]:
        raise IndexError("Index out of range")

    current = lst["first"]
    prev = None

    for i in range(index):
        prev = current
        current = current["next"]

    # current es el nodo a eliminar
    if prev is None:
        # eliminar el primero
        lst["first"] = current["next"]
        if lst["size"] == 1:   # si solo había un nodo
            lst["last"] = None
    else:
        prev["next"] = current["next"]
        if current == lst["last"]:  # si era el último
            lst["last"] = prev

    lst["size"] -= 1
    return current["info"]
