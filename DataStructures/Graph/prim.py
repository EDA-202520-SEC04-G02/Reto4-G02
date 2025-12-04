from DataStructures.List import array_list as lt
from DataStructures.Map import map_linear_probing as mp
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Graph import digraph as G
from DataStructures.Graph import vertex as vtx


def new_prim_structure(source, visited, heap):
    """
    Crea la estructura resultado de Prim.
    """
    return {
        "source": source,
        "visited": visited,
        "pq": heap
    }


def prim_mst(my_graph, source):
    """
    Implementa el algoritmo de Prim para encontrar el árbol de expansión mínima (MST)
    del componente conectado que contiene a `source`.

    Retorna una estructura:
    {
        "source": source,
        "visited": map(key -> {"edge_from", "dist_to", "marked"}),
        "pq": heap_de_prioridad
    }
    """
    # Tamaño sugerido para el mapa de visitados
    order = G.order(my_graph)
    visited = mp.new_map(order, 0.5)

    # Cola de prioridad: min-heap (True => menor prioridad = mejor)
    heap = pq.new_heap(True)

    # Inicializar el vértice fuente
    mp.put(visited, source, {
        "edge_from": None,   # de dónde viene en el MST
        "dist_to": 0.0,      # peso de la arista que lo conecta al MST
        "marked": False      # ya está definitivo en el MST o no
    })
    pq.insert(heap, source, 0.0)

    # Bucle principal de Prim
    while pq.size(heap) > 0:
        # Extraer el vértice con menor "dist_to"
        u = pq.remove(heap)

        info_u = mp.get(visited, u)
        if info_u is None:
            # seguridad extra, no debería pasar
            continue

        if info_u.get("marked", False):
            # si ya estaba marcado, es una entrada vieja de la cola
            continue

        # Fijar u como parte definitiva del MST
        info_u["marked"] = True
        mp.put(visited, u, info_u)

        # Relajar aristas u -> v
        adj = G.adjacents(my_graph, u)  # lista con llaves de vértice v
        num_adj = lt.size(adj)

        for i in range(num_adj):
            v = lt.get_element(adj, i)

            # Obtener el arco (u, v) y su peso
            vert_u = G.get_vertex(my_graph, u)
            edge_uv = vtx.get_edge(vert_u, v)
            if edge_uv is None:
                continue

            weight = edge_uv["weight"]

            info_v = mp.get(visited, v)

            if info_v is None:
                # v no se ha visto: lo agregamos como candidato al MST
                mp.put(visited, v, {
                    "edge_from": u,
                    "dist_to": weight,
                    "marked": False
                })
                pq.insert(heap, v, weight)

            else:
                # v ya es candidato pero aún no está fijado
                if not info_v.get("marked", False) and weight < info_v["dist_to"]:
                    # Encontramos un mejor arco que conecta v al MST
                    info_v["edge_from"] = u
                    info_v["dist_to"] = weight
                    mp.put(visited, v, info_v)

                    # Actualizar la prioridad en la cola
                    if pq.contains(heap, v):
                        pq.improve_priority(heap, v, weight)
                    else:
                        pq.insert(heap, v, weight)

    return new_prim_structure(source, visited, heap)


def edges_mst(my_graph, aux_structure):
    """
    Retorna una lista (array_list) con los arcos del MST.

    Cada elemento es un diccionario:
    {
        "edge_from": u,
        "to": v,
        "dist_to": peso_arista_u_v
    }

    Precondición: se debe haber ejecutado prim_mst.
    """
    visited = aux_structure["visited"]
    edges = lt.new_list()

    vertices = mp.key_set(visited)
    num_vertices = lt.size(vertices)

    for i in range(num_vertices):
        v = lt.get_element(vertices, i)
        info_v = mp.get(visited, v)
        if info_v is None:
            continue

        u = info_v["edge_from"]
        if u is None:
            # La raíz (source) no tiene arista de entrada
            continue

        edge_info = {
            "edge_from": u,
            "to": v,
            "dist_to": info_v["dist_to"]
        }
        lt.add_last(edges, edge_info)

    return edges


def weight_mst(my_graph, aux_structure):
    """
    Retorna el peso total del árbol de expansión mínima (MST).

    Precondición: se debe haber ejecutado prim_mst.
    """
    visited = aux_structure["visited"]
    total_weight = 0.0

    vertices = mp.key_set(visited)
    num_vertices = lt.size(vertices)

    for i in range(num_vertices):
        v = lt.get_element(vertices, i)
        info_v = mp.get(visited, v)
        if info_v is None:
            continue

        # Solo sumamos las aristas que tienen origen (no la raíz)
        if info_v["edge_from"] is not None:
            total_weight += info_v["dist_to"]

    return total_weight
