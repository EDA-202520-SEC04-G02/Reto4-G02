from DataStructures.Map import map_linear_probing as map
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import array_list as lt
from DataStructures.Graph import digraph as G
from DataStructures.Graph import vertex as vtx
from DataStructures.Graph import prim_structure as prim_st
# OJO: ya no usamos edge.get_weight, así que no es obligatorio importar edge
# from DataStructures.Graph import edge as edg


def prim_mst(my_graph, source):
    """
    Implementa el algoritmo de Prim para encontrar el árbol de expansión mínima (MST)
    de un grafo (representado como no dirigido usando arcos en ambos sentidos).

    Retorna la estructura prim_structure creada con new_prim_structure, que contiene:
      - source
      - edge_from: mapa v -> vértice padre en el MST
      - dist_to:   mapa v -> peso de la arista que conecta con el MST
      - marked:    mapa de vértices ya incluidos en el MST
      - pq:        heap de prioridad mínima sobre vértices (por dist_to)
    """
    # Si el vértice fuente no existe en el grafo, retornamos estructura vacía
    if not G.contains_vertex(my_graph, source):
        order = G.order(my_graph)
        return prim_st.new_prim_structure(source, order)

    order = G.order(my_graph)
    structure = prim_st.new_prim_structure(source, order)

    edge_from = structure["edge_from"]
    dist_to   = structure["dist_to"]
    marked    = structure["marked"]
    heap      = structure["pq"]

    # Inicializar dist_to y edge_from para todos los vértices
    vertices = G.vertices(my_graph)  # array_list de llaves de vértice
    n = lt.size(vertices)
    for i in range(n):
        v = lt.get_element(vertices, i)
        map.put(dist_to, v, float("inf"))
        map.put(edge_from, v, None)

    # Para la fuente: distancia 0
    map.put(dist_to, source, 0.0)
    pq.insert(heap, 0.0, source)

    # Bucle principal de Prim
    while not pq.is_empty(heap):
        # Tomar el vértice con menor dist_to
        v = pq.remove(heap)

        # Puede haber entradas duplicadas con prioridades viejas
        if map.contains(marked, v):
            continue

        # Marcar como parte del MST
        map.put(marked, v, True)

        vertex_v = G.get_vertex(my_graph, v)
        if vertex_v is None:
            continue

        # Recorrer adyacentes de v
        adj = G.adjacents(my_graph, v)  # array_list de llaves destino
        m = lt.size(adj)
        for i in range(m):
            w = lt.get_element(adj, i)

            # Si w ya está en el MST, lo ignoramos
            if map.contains(marked, w):
                continue

            # Obtener la arista v -> w desde el vértice v
            edge = vtx.get_edge(vertex_v, w)
            if edge is None:
                continue

            # ⚠️ AQUÍ ESTABA EL PROBLEMA: no existe edg.get_weight(edge)
            # En tu TDA edge el peso se guarda como edge["weight"]
            weight = edge["weight"]

            # Distancia actual conocida a w
            current = map.get(dist_to, w)
            if current is None or weight < current:
                # Mejorar conexión de w al MST
                map.put(dist_to, w, weight)
                map.put(edge_from, w, v)

                if pq.contains(heap, w):
                    pq.improve_priority(heap, w, weight)
                else:
                    pq.insert(heap, weight, w)

    return structure


def edges_mst(my_graph, aux_structure):
    """
    Retorna los arcos del árbol de expansión mínima (MST) como una lista TDA (array_list).

    Cada elemento de la lista es un diccionario:
      {
        "edge_from": <v_origen>,
        "to":        <v_destino>,
        "dist_to":   <peso_arista>
      }

    Precondición: se debió ejecutar previamente prim_mst(my_graph, source).
    """
    edge_from = aux_structure["edge_from"]
    dist_to   = aux_structure["dist_to"]
    marked    = aux_structure["marked"]

    result = lt.new_list()

    vertices = G.vertices(my_graph)
    n = lt.size(vertices)

    for i in range(n):
        v = lt.get_element(vertices, i)

        # Solo consideramos vértices que quedaron en el MST (marcados)
        if not map.contains(marked, v):
            continue

        parent = map.get(edge_from, v)
        if parent is None:
            # Raíz del MST o vértice no conectado
            continue

        d = map.get(dist_to, v)
        if d is None:
            d = 0.0

        info = {
            "edge_from": parent,
            "to": v,
            "dist_to": d
        }
        lt.add_last(result, info)

    return result


def weight_mst(my_graph, aux_structure):
    """
    Retorna el peso total del árbol de expansión mínima (MST).

    Precondición: se debió ejecutar previamente prim_mst(my_graph, source).
    """
    edges = edges_mst(my_graph, aux_structure)
    total = 0.0

    n = lt.size(edges)
    for i in range(n):
        info = lt.get_element(edges, i)
        total += info["dist_to"]

    return total
