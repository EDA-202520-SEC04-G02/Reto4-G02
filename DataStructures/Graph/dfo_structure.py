from DataStructures.Queue import queue
from DataStructures.Stack import stack
from DataStructures.Map import map_linear_probing as map


def new_dfo_structure(g_order):
    """
    Crea una estructura de busqueda usada en el algoritmo **depth_first_order**.

    Se crea una estructura de busqueda con los siguientes atributos:

    - **marked**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **pre**: Cola con los vertices visitados en preorden. Se inicializa como una cola vacia.
    - **post**: Cola con los vertices visitados en postorden. Se inicializa como una cola vacia.
    - **reversepost**: Pila con los vertices visitados en postorden inverso. Se inicializa como una pila vacia.

    :returns: Estructura de busqueda
    :rtype: dfo_search
    """
    dfo_structure = {
        'marked': None,
        'pre': queue.new_queue(),
        'post': queue.new_queue(),
        'reversepost': stack.new_stack()
    }
    dfo_structure["marked"] = map.new_map(
        num_elements=g_order, load_factor=0.5
    )
    return dfo_structure
