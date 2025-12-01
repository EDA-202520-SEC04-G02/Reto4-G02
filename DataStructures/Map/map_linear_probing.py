from DataStructures.List import array_list as al
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf

# Funciones extras (Sacadas de https://isis1225devs.github.io/ISIS1225-Structure-Documentation/DataStructures.Map.html#map-linear-probing-py)

def find_slot(my_map, key, hash_value):
   first_avail = None
   found = False
   ocupied = False
   while not found:
      if is_available(my_map["table"], hash_value):
            if first_avail is None:
               first_avail = hash_value
            entry = al.get_element(my_map["table"], hash_value)
            if me.get_key(entry) is None:
               found = True
      elif default_compare(key, al.get_element(my_map["table"], hash_value)) == 0:
            first_avail = hash_value
            found = True
            ocupied = True
      hash_value = (hash_value + 1) % my_map["capacity"]
   return ocupied, first_avail

def default_compare(key, entry):

   if key == me.get_key(entry):
      return 0
   elif key > me.get_key(entry):
      return 1
   return -1

def is_available(table, pos):

   entry = al.get_element(table, pos)
   if me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
      return True
   return False

# Funciones adicionales no dadas

def rehash(my_map):

    # nueva capacidad
    new_capacity = mf.next_prime(2 * my_map["capacity"])

    # crear nueva tabla vacía con mismo prime y limit_factor
    new_map_obj = new_map(new_capacity, my_map["limit_factor"], my_map["prime"])

    # reinsertar elementos válidos
    for i in range(al.size(my_map["table"])):
        entry = al.get_element(my_map["table"], i)
        if entry is not None and me.get_key(entry) not in (None, "__EMPTY__"):
            put(new_map_obj, me.get_key(entry), me.get_value(entry))

    return new_map_obj

def get(my_map, key):
    start = mf.hash_value(my_map, key)
    ocupied, pos = find_slot(my_map, key, start)
    if ocupied and pos is not None:
        entry = al.get_element(my_map["table"], pos)
        return me.get_value(entry)
    return None


def contains(my_map, key):
    start = mf.hash_value(my_map, key)
    ocupied, pos = find_slot(my_map, key, start)
    return ocupied

# Funciones requeridas

def new_map(num_elements, load_factor, prime=109345121):

    if load_factor <= 0:
        raise ValueError("El load_factor debe ser mayor que 0")

    # capacidad = siguiente primo >= num_elements / load_factor
    capacity = mf.next_prime(int(num_elements / load_factor))

    # crear array_list vacío
    table = al.new_list()

    # llenarlo con entradas None-None
    for _ in range(capacity):
        al.add_last(table, me.new_map_entry(None, None))

    # devolver estructura del mapa
    return {
        "prime": prime,
        "capacity": capacity,
        "scale": 1,   # fijo por requerimientos de pruebas
        "shift": 0,   # fijo por requerimientos de pruebas
        "table": table,
        "current_factor": 0.0,
        "limit_factor": load_factor,
        "size": 0,
    }

def put(my_map, key, value):

    # 1. hash de la llave
    start = mf.hash_value(my_map, key)

    # 2. buscar posición
    ocupied, pos = find_slot(my_map, key, start)

    if ocupied:
        # 3a. clave ya existe → actualizar valor
        al.get_element(my_map["table"], pos)["value"] = value
    else:
        # 3b. clave nueva → insertar entrada
        al.change_element(my_map["table"], pos, me.new_map_entry(key, value))
        my_map["size"] += 1

    # 4. actualizar factor de carga
    my_map["current_factor"] = my_map["size"] / my_map["capacity"]

    # 5. rehash si excede límite
    if my_map["current_factor"] > my_map["limit_factor"]:
        my_map = rehash(my_map)

    # 6. retornar tabla actualizada
    return my_map

def remove(my_map, key):

    h = mf.hash_value(my_map, key)
    ocupied, pos = find_slot(my_map, key, h)

    if ocupied and pos is not None:
        # marcar como eliminado
        al.change_element(my_map["table"], pos, {"key": "__EMPTY__", "value": "__EMPTY__"})
        my_map["size"] -= 1
        # recalcular factor de carga
        my_map["current_factor"] = my_map["size"] / my_map["capacity"]

    return my_map

def size(my_map):

    return my_map["size"]

def is_empty(my_map):

    return my_map["size"] == 0
def key_set(my_map):

    keys = al.new_list()
    for i in range(al.size(my_map["table"])):
        entry = al.get_element(my_map["table"], i)
        if entry is not None and me.get_key(entry) not in (None, "__EMPTY__"):
            al.add_last(keys, me.get_key(entry))
    return keys
def value_set(my_map):

    values = al.new_list()
    for i in range(al.size(my_map["table"])):
        entry = al.get_element(my_map["table"], i)
        if entry is not None and me.get_key(entry) not in (None, "__EMPTY__"):
            al.add_last(values, me.get_value(entry))
    return values


