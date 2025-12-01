from DataStructures.Map import map_linear_probing as map
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Queue import queue as q


def new_prim_structure(source, g_order):
    """
    Crea una estructura de busqueda usada en el algoritmo **prim**.

    Se crea una estructura de busqueda con los siguientes atributos:

    - **source**: Vertice de inicio del MST.
    - **edge_from**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **dist_to**: Mapa con las distancias a los vertices. Se inicializa en ``None``
    - **marked**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **pq**: Cola de prioridad indexada (index_priority_queue). Se inicializa en ``None``

    :returns: Estructura de busqueda
    :rtype: prim_search
    """

    structure = {
        "source": source,
        "edge_from": map.new_map(g_order, 0.5),
        "dist_to": map.new_map(g_order, 0.5),
        "marked": map.new_map(g_order, 0.5),
        "pq":  pq.new_heap(),
    }

    return structure
