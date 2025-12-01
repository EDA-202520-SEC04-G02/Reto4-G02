from DataStructures.Graph import digraph as G
from DataStructures.Graph import edge as edg
from DataStructures.Map import map_linear_probing as map
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import array_list as al
from DataStructures.List import single_linked_list as lt
import math



def new_dijsktra_structure(source, g_order):
    """

    Crea una estructura de busqueda usada en el algoritmo **dijsktra**.

    Se crea una estructura de busqueda con los siguientes atributos:

    - **source**: Vertice de origen. Se inicializa en ``source``
    - **visited**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **pq**: Cola indexada con los vertices visitados. Se inicializa en ``None``

    :returns: Estructura de busqueda
    :rtype: dijsktra_search
    """
    structure = {
        "source": source,
        "visited": map.new_map(
            g_order, 0.5),
        "pq": pq.new_heap()}
    return structure


def dijkstra(my_graph, source):
    """
    Implementa el algoritmo de Dijkstra para encontrar los caminos
    más baratos desde el vértice source hasta todos los vértices
    alcanzables en el grafo my_graph.

    Usa new_dijsktra_structure(source, g_order) para crear la
    estructura base y luego rellena 'visited' y 'pq'.
    """

    
    n_vertices = G.order(my_graph)

    
    structure = new_dijsktra_structure(source, n_vertices)
    visited = structure["visited"]
    pq_struct = structure["pq"]

    
    vkeys = G.vertices(my_graph)        
    total = al.size(vkeys)
    idx = 0
    while idx < total:
        key_v = al.get_element(vkeys, idx)
        map.put(visited, key_v, {
            "marked": False,
            "edge_from": None,
            "dist_to": math.inf
        })
        idx += 1

    
    if not map.contains(visited, source):
        map.put(visited, source, {
            "marked": False,
            "edge_from": None,
            "dist_to": 0.0
        })
    else:
        info = map.get(visited, source)
        info["marked"] = False
        info["edge_from"] = None
        info["dist_to"] = 0.0
        map.put(visited, source, info)

    
    pq.insert(pq_struct, 0.0, source)

   
    while not pq.is_empty(pq_struct):

        
        v = pq.remove(pq_struct)

        v_info = map.get(visited, v)
        if v_info is None:
            v_info = {
                "marked": False,
                "edge_from": None,
                "dist_to": math.inf
            }
            map.put(visited, v, v_info)

        if not v_info["marked"]:
            
            v_info["marked"] = True
            map.put(visited, v, v_info)

            
            edges_map = G.edges_vertex(my_graph, v)

            if edges_map is not None:
                adj_keys = map.key_set(edges_map)   
                total_adj = al.size(adj_keys)
                j = 0

                while j < total_adj:
                    w = al.get_element(adj_keys, j)
                    edge_vw = map.get(edges_map, w)

                    if edge_vw is not None:
                        weight_vw = edg.weight(edge_vw)

                        if weight_vw is not None:
                            w_info = map.get(visited, w)
                            if w_info is None:
                                w_info = {
                                    "marked": False,
                                    "edge_from": None,
                                    "dist_to": math.inf
                                }

                            
                            new_dist = v_info["dist_to"] + weight_vw

                            if new_dist < w_info["dist_to"]:
                                w_info["dist_to"] = new_dist
                                w_info["edge_from"] = v
                                map.put(visited, w, w_info)

                                if pq.contains(pq_struct, w):
                                    pq.improve_priority(pq_struct, w, new_dist)
                                else:
                                    pq.insert(pq_struct, new_dist, w)

                    j += 1

    
    structure["visited"] = visited
    structure["pq"] = pq_struct
    return structure


def dist(key_v, visited):
    """
    Función auxiliar: retorna la distancia almacenada en visited
    para el vértice key_v. Si no existe, retorna infinito.
    """
    exists = map.contains(visited, key_v)
    if not exists:
        return math.inf
    info = map.get(visited, key_v)
    return info["dist_to"]


def dist_to(key_v, aux_structure):
    """
    Retorna el costo del camino mínimo entre source y key_v.
    """
    return dist(key_v, aux_structure["visited"])


def has_path_to(key_v, aux_structure):
    """
    Indica si existe camino entre source y key_v.
    """
    return dist_to(key_v, aux_structure) != math.inf


def path_to(key_v, aux_structure):
    """
    Retorna el camino entre source y key_v en una lista enlazada simple (lt).
    Si no hay camino, retorna None.
    """
    exists = has_path_to(key_v, aux_structure)
    if not exists:
        return None

    visited = aux_structure["visited"]
    path = lt.new_list()
    current = key_v

    while current is not None:
        lt.add_last(path, current)
        info = map.get(visited, current)
        current = info["edge_from"]

    return path


