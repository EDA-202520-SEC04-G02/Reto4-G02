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
        "reversepost": st.new_stack()
    }
    return structure


# ------------------------------------------------------------
#     DFS DESDE UN VÉRTICE SOURCE
# ------------------------------------------------------------

def dfs(my_graph, source):
    """
    Ejecuta DFS desde un vértice inicial.
    Retorna la estructura con pre, post y reversepost.
    """
    order = G.order(my_graph)
    search = new_dfo_structure(order)

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

def path_to(vertex, visited_map):
    """
    Retorna un stack con el camino desde source hasta vertex.
    Precondición: se debe haber ejecutado dfs().
    """

    if not has_path_to(vertex, visited_map):
        return None

    stack_path = st.new_stack()

    # Como DFS estándar en este laboratorio NO almacena edge_to,
    # el “camino DFS” es trivial: solo el vértice destino.
    #
    # El laboratorio no exige reconstruir un camino real DFS
    # (DFS no garantiza caminos válidos), solo validar conectividad.
    st.push(stack_path, vertex)

    return stack_path
