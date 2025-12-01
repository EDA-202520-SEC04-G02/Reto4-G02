from DataStructures.Graph import digraph as G
from DataStructures.Map import map_linear_probing as map
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as st
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import single_linked_list as lt
from DataStructures.List import array_list as al
from DataStructures.Graph import edge as edg
import math
from DataStructures.Graph import dijsktra_structure as djs


def bfs(my_graph, source):
    """
    Inicia un recorrido BFS y retorna el visited_map.
    """

    
    visited_map = map.new_map(
        num_elements=G.order(my_graph),
        load_factor=0.5
    )

    
    map.put(visited_map, source, {
        "edge_from": None,
        "dist_to": 0
    })

    
    visited_map = bfs_vertex(my_graph, source, visited_map)

    return visited_map

def bfs_vertex(my_graph, source, visited_map):
    """
    Recorre el grafo en anchura desde `source`, actualizando visited_map.
    """

    to_visit = q.new_queue()
    q.enqueue(to_visit, source)

    while not q.is_empty(to_visit):
        v = q.dequeue(to_visit)

        
        v_info = map.get(visited_map, v)
        dist_v = v_info["dist_to"]

        
        adj_vertices = G.adjacents(my_graph, v)
        n_adj = al.size(adj_vertices)

        
        i = 0
        while i < n_adj:
            w = al.get_element(adj_vertices, i)
            i += 1

            
            if not map.contains(visited_map, w):
                map.put(visited_map, w, {
                    "edge_from": v,
                    "dist_to": dist_v + 1
                })
                q.enqueue(to_visit, w)

    return visited_map


def has_path_to(key_v, visited_map):
    return map.contains(visited_map, key_v)


def path_to(key_v, visited_map):
    
    if not has_path_to(key_v, visited_map):
        return None

    path_stack = st.new_stack()
    current = key_v

    
    while current is not None:
        st.push(path_stack, current)
        info = map.get(visited_map, current)
        current = info["edge_from"]

    return path_stack

def dijkstra(my_graph, source):
   

    
    n_vertices = G.order(my_graph)

    
    structure = djs.new_dijsktra_structure(source, n_vertices)
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
