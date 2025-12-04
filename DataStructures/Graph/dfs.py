from DataStructures.Map import map_linear_probing as map
from DataStructures.Stack import stack as st
from DataStructures.Queue import queue as q
from DataStructures.List import array_list as al
from DataStructures.List import single_linked_list as lt
from DataStructures.Graph import vertex as vtx
from DataStructures.Graph import digraph as G


# ------------------------------------------------------------
#     CREAR ESTRUCTURA DFS
# ------------------------------------------------------------

def new_dfo_structure(g_order):
    structure = {
        "marked": map.new_map(g_order, 0.5),
        "pre": q.new_queue(),
        "post": q.new_queue(),
        "reversepost": st.new_stack(),
        # NUEVO: para reconstruir caminos
        "edge_to": map.new_map(g_order, 0.5),
        "source": None   # lo llenamos en dfs()
    }
    return structure



# ------------------------------------------------------------
#     DFS DESDE UN VÉRTICE SOURCE
# ------------------------------------------------------------

def dfs(my_graph, source):
    """
    Ejecuta DFS desde un vértice inicial.
    Retorna la estructura con pre, post, reversepost y edge_to.
    """
    order = G.order(my_graph)
    search = new_dfo_structure(order)
    search["source"] = source   # guardar vértice origen

    dfs_vertex(my_graph, source, search)
    return search



# ------------------------------------------------------------
#     DFS RECURSIVO
# ------------------------------------------------------------

def dfs_vertex(my_graph, vertex, search):
    """
    Función recursiva DFS.
    """

    # marcar vértice como visitado
    map.put(search["marked"], vertex, True)

    # agregar al preorden
    q.enqueue(search["pre"], vertex)

    # obtener adyacentes
    adj = G.adjacents(my_graph, vertex)  # array_list

    for i in range(al.size(adj)):
        w = al.get_element(adj, i)
        if not map.contains(search["marked"], w):
            # NUEVO: registrar que llegamos a w desde vertex
            map.put(search["edge_to"], w, vertex)
            dfs_vertex(my_graph, w, search)

    # agregar al postorden
    q.enqueue(search["post"], vertex)

    # agregar a reverse post (stack)
    st.push(search["reversepost"], vertex)

    return search



# ------------------------------------------------------------
#     ¿EXISTE CAMINO A vertex?
# ------------------------------------------------------------

def has_path_to(vertex, visited_map):
    """
    Retorna True si DFS visitó el vértice `vertex`.
    """
    return map.contains(visited_map["marked"], vertex)


# ------------------------------------------------------------
#     OBTENER CAMINO DESDE SOURCE A VERTEX
# ------------------------------------------------------------

def path_to(vertex, search):
    """
    Retorna un stack con el camino desde source hasta vertex.
    Precondición: se debe haber ejecutado dfs().
    `search` es la estructura retornada por dfs().
    """

    if not has_path_to(vertex, search):
        return None

    stack_path = st.new_stack()
    source = search["source"]
    current = vertex

    # Reconstruir camino vertex -> source usando edge_to,
    # pero guardarlo en un stack para que al hacer pop salga en orden source -> vertex.
    while current is not None and current != source:
        st.push(stack_path, current)
        current = map.get(search["edge_to"], current)

    # agregar el source al principio del camino
    if current == source:
        st.push(stack_path, source)

    return stack_path

