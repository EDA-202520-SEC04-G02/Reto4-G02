from DataStructures.List import array_list as al
from DataStructures.List import single_linked_list as sl
import DataStructures.Map.map_functions as mf
from DataStructures.Map import map_entry as me

def new_map(num_elements, load_factor, prime=109345121):
    if load_factor <= 0:
        raise ValueError("El load_factor debe ser mayor que 0")

    # Capacidad mínima para cumplir con el factor de carga
    capacity = mf.next_prime(int(num_elements / load_factor))
    # Crear la tabla como un array_list de buckets (listas enlazadas vacías)
    table = al.new_list()
    for _ in range(capacity):
        bucket = sl.new_list()
        al.add_last(table, bucket)

    return {
        "prime": prime,
        "capacity": capacity,
        "scale": 1,   # fijo para pruebas
        "shift": 0,   # fijo para pruebas
        "table": table,
        "current_factor": 0.0,
        "limit_factor": load_factor,
        "size": 0,
    }

def default_compare(key, entry):
    """
    Compara una llave con la llave de una entrada de mapa.
    Retorna 0 si son iguales, 1 si key > entry.key, -1 en caso contrario.
    """
    entry_key = me.get_key(entry)
    if key == entry_key:
        return 0
    elif key > entry_key:
        return 1
    return -1

def rehash(my_map):
    """
    Realiza un rehashing de la tabla con separate chaining.
    """
    old_table = my_map["table"]
    old_capacity = my_map["capacity"]

    # nueva capacidad (forzar > old_capacity)
    new_capacity = mf.next_prime(2 * old_capacity)
    if new_capacity <= old_capacity:
        new_capacity = mf.next_prime(old_capacity * 2 + 1)

    # crear nueva tabla vacía
    new_map_obj = new_map(new_capacity, my_map["limit_factor"], my_map["prime"])

    # recorrer cada bucket y reinsertar todos los elementos
    for i in range(al.size(old_table)):
        bucket = al.get_element(old_table, i)
        for j in range(sl.size(bucket)):
            entry = sl.get_element(bucket, j)
            if entry is not None and me.get_key(entry) is not None:
                put(new_map_obj, me.get_key(entry), me.get_value(entry))

    # copiar la referencia de la nueva tabla al objeto original
    my_map.update(new_map_obj) # Así es más rápido que hacerlo uno por uno

    return my_map


def put(my_map, key, value):
    """
    Agrega una nueva entrada llave-valor a la tabla con separate chaining.
    Si la llave ya existe, se actualiza su valor.
    """
    pos = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], pos)

    # Buscar si ya existe la clave
    for i in range(sl.size(bucket)):
        entry = sl.get_element(bucket, i)
        if me.get_key(entry) == key:
            # actualizar valor
            entry["value"] = value
            return my_map  # no cambia size ni factor

    # insertar nueva entrada
    new_entry = me.new_map_entry(key, value)
    sl.add_last(bucket, new_entry)
    my_map["size"] += 1
    my_map["current_factor"] = my_map["size"] / my_map["capacity"]

    # verificar rehash
    if my_map["current_factor"] > my_map["limit_factor"]:
        return rehash(my_map)

    return my_map

def contains(my_map, key):
    pos = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], pos)
    # recorrer la lista en ese bucket
    for i in range(sl.size(bucket)):
        entry = sl.get_element(bucket, i)
        if me.get_key(entry) == key:
            return True
    return False

def get(my_map, key):
    pos = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], pos)
    for i in range(sl.size(bucket)):
        entry = sl.get_element(bucket, i)
        if me.get_key(entry) == key:
            return me.get_value(entry)
    # si no se encontró
    return None

def remove(my_map, key):
    pos = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], pos)
    for i in range(sl.size(bucket)):
        entry = sl.get_element(bucket, i)
        if me.get_key(entry) == key:
            removed = sl.remove_at(bucket, i)  # ahora sí existe
            my_map["size"] -= 1
            my_map["current_factor"] = my_map["size"] / my_map["capacity"]
            return me.get_value(removed)
    return None

def size(my_map):
    return my_map["size"]

def is_empty(my_map):
    return my_map["size"] == 0

def key_set(my_map):
    keys = al.new_list()  
    table = my_map["table"]
    for i in range(al.size(table)):
        bucket = al.get_element(table, i)
        for j in range(sl.size(bucket)):
            entry = sl.get_element(bucket, j)
            al.add_last(keys, me.get_key(entry))
    return keys


def value_set(my_map):
    values = al.new_list()  
    table = my_map["table"]
    for i in range(al.size(table)):
        bucket = al.get_element(table, i)
        for j in range(sl.size(bucket)):
            entry = sl.get_element(bucket, j)
            al.add_last(values, me.get_value(entry))
    return values

