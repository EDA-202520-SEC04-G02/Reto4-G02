from DataStructures.Map import map_linear_probing as mp
from DataStructures.Graph import vertex as vtx
from DataStructures.Graph import edge as edg
from DataStructures.List import array_list as al


# ---------------------------------------------------
#   Crear un grafo dirigido vacío
# ---------------------------------------------------
def new_graph(order):
    """
    Crea un grafo dirigido vacío.

    Atributos del grafo:
        - vertices: mapa LP con los vértices (key → vertex)
        - num_edges: número total de arcos

    :param order: tamaño inicial del mapa de vértices
    """
    graph = {
        "vertices": mp.new_map(order, 0.5),
        "num_edges": 0
    }
    return graph


# ---------------------------------------------------
#   Insertar un vértice
# ---------------------------------------------------
def insert_vertex(graph, key, value=None):
    """
    Inserta un nuevo vértice con clave `key` en el grafo.
    Si ya existe, no hace nada.

    El vértice se crea usando new_vertex(key, value)
    """
    if not mp.contains(graph["vertices"], key):
        vertex = vtx.new_vertex(key, value)
        graph["vertices"] = mp.put(graph["vertices"], key, vertex)

    return graph


# ---------------------------------------------------
#   Verificar si un vértice existe
# ---------------------------------------------------
def contains_vertex(graph, key):
    """
    Retorna True si el grafo contiene el vértice con clave `key`.
    """
    return mp.contains(graph["vertices"], key)


# ---------------------------------------------------
#   Añadir un arco dirigido (key_a → key_b)
# ---------------------------------------------------
def add_edge(graph, key_a, key_b, weight):
    """
    Agrega un arco desde key_a hacia key_b con peso `weight`.

    - Si el vértice no existe, no se agrega nada.
    - Se incrementa num_edges solo si el arco NO existía.
    """
    if not contains_vertex(graph, key_a):
        return graph
    if not contains_vertex(graph, key_b):
        return graph

    vertex_a = mp.get(graph["vertices"], key_a)

    # Revisar si el arco ya existía
    old_edge = vtx.get_edge(vertex_a, key_b)
    if old_edge is None:
        # Crear arco nuevo
        vtx.add_adjacent(vertex_a, key_b, weight)
        graph["num_edges"] += 1
    else:
        # Actualizar peso
        edg.set_weight(old_edge, weight)

    # Guardar cambios en el mapa
    graph["vertices"] = mp.put(graph["vertices"], key_a, vertex_a)
    return graph


# ---------------------------------------------------
#   Número de vértices
# ---------------------------------------------------
def order(graph):
    """
    Retorna el número de vértices del grafo.
    """
    return mp.size(graph["vertices"])


# ---------------------------------------------------
#   Número de arcos
# ---------------------------------------------------
def size(graph):
    """
    Retorna el número total de arcos del grafo.
    """
    return graph["num_edges"]


# --------------------------------------------------------------------------------------------------

def get_vertex(graph, key):
    """
    Retorna el vértice con clave key o None si no existe.
    """
    return mp.get(graph["vertices"], key)


def vertices(graph):
    """
    Retorna una lista/iterable con TODAS las claves de los vértices.
    """
    return mp.key_set(graph["vertices"])


def degree(graph, key):
    """
    Grado del vértice: número de arcos salientes.
    """
    vertex = get_vertex(graph, key)
    return vtx.degree(vertex)


def adjacents(graph, key):
    """
    Retorna las llaves de los vértices adyacentes a key.
    """
    vertex = get_vertex(graph, key)
    if vertex is None:
        return al.new_list()

    adj_map = vtx.get_adjacents(vertex)
    if adj_map is None:
        return al.new_list()

    return mp.key_set(adj_map)

def edges_vertex(graph, key):
    """
    Retorna el mapa de arcos del vértice con clave key.
    """
    vertex = get_vertex(graph, key)
    return vtx.get_adjacents(vertex)


def update_vertex_info(graph, key, new_value):
    """
    Cambia el valor almacenado en el vértice.
    """
    vertex = get_vertex(graph, key)
    if vertex:
        vtx.set_value(vertex, new_value)
    return graph


def get_vertex_information(graph, key):
    """
    Retorna el valor del vértice.
    """
    vertex = get_vertex(graph, key)
    return vtx.get_value(vertex) if vertex else None