from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Priority_queue import pq_entry as pqe
from DataStructures.List import array_list as al # Nah no toca usarlo


def new_heap(is_min_pq = True):
    heap = {
        "elements": {
            "elements": [None],  # lista para almacenar los elementos del heap
            "size": 1            # tamaño del heap
        },      # diccionario para almacenar los elementos del heap
        "size": 0,                    # tamaño del heap
        "cmp_function": None,        # función de comparación
    }

    if is_min_pq:
        heap["cmp_function"] = default_compare_lower_value
    else:
        heap["cmp_function"] = default_compare_higher_value

    return heap


def default_compare_higher_value(father_node, child_node):
    if pqe.get_priority(father_node) >= pqe.get_priority(child_node):
        return True
    return False

def default_compare_lower_value(father_node, child_node):
    if pqe.get_priority(father_node) <= pqe.get_priority(child_node):
        return True
    return False

def priority(my_heap, parent, child):
    return my_heap['cmp_function'](parent, child)

def size(my_heap):
    return my_heap['size']

def exchange(my_heap, pos1, pos2):
    elements = my_heap["elements"]["elements"]
    elements[pos1], elements[pos2] = elements[pos2], elements[pos1]
    
def is_empty(my_heap):
    return size(my_heap) == 0

def swim(my_heap, pos):
    elements = my_heap["elements"]["elements"]

    # Mientras el nodo no sea la raíz y tenga mayor prioridad que su padre
    while pos > 1:
        parent = pos // 2

        # Si el padre ya tiene mayor prioridad, se detiene
        if priority(my_heap, elements[parent], elements[pos]):
            break

        # Si no, intercambiamos los elementos
        exchange(my_heap, parent, pos)

        # Subimos al padre
        pos = parent
        
        
from DataStructures.Priority_queue import pq_entry as pqe

def insert(my_heap, priority, value):
    
    # Crear una nueva entrada de prioridad
    new_entry = pqe.new_pq_entry(priority, value)

    # Añadir la entrada al final del heap
    my_heap["elements"]["elements"].append(new_entry)
    my_heap["elements"]["size"] += 1
    my_heap["size"] += 1

    # Posición del nuevo elemento (última)
    pos = my_heap["elements"]["size"] - 1

    # Reordenar hacia arriba para mantener la propiedad del heap
    swim(my_heap, pos)

    return my_heap

def sink(my_heap, pos):
    elems = my_heap["elements"]["elements"]
    size = my_heap["elements"]["size"] - 1
    while 2 * pos <= size:
        j = 2 * pos
        if j < size and not priority(my_heap, elems[j], elems[j + 1]):
            j += 1
        if priority(my_heap, elems[pos], elems[j]):
            break
        exchange(my_heap, pos, j)
        pos = j

def remove(my_heap):
    if is_empty(my_heap):
        return None
    from DataStructures.Priority_queue import pq_entry as pqe
    elems = my_heap["elements"]["elements"]
    size = my_heap["elements"]["size"] - 1
    root = elems[1]
    elems[1] = elems[size]
    elems.pop()
    my_heap["elements"]["size"] -= 1
    my_heap["size"] -= 1
    if my_heap["size"] > 0:
        sink(my_heap, 1)
    return pqe.get_value(root)


def is_present_value(my_heap, value):
    from DataStructures.Priority_queue import pq_entry as pqe
    for i in range(1, my_heap["elements"]["size"]):
        if pqe.get_value(my_heap["elements"]["elements"][i]) == value:
            return i
    return -1


def get_first_priority(my_heap):
    if is_empty(my_heap):
        return None
    from DataStructures.Priority_queue import pq_entry as pqe
    return pqe.get_priority(my_heap["elements"]["elements"][1])

def contains(my_heap, value):
    return is_present_value(my_heap, value) != -1

def improve_priority(my_heap, value, new_priority):
    from DataStructures.Priority_queue import pq_entry as pqe
    elems = my_heap["elements"]["elements"]
    for i in range(1, my_heap["elements"]["size"]):
        if pqe.get_value(elems[i]) == value:
            pqe.set_priority(elems[i], new_priority)
            swim(my_heap, i)
            break
