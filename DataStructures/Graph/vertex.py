from DataStructures.Map import map_linear_probing as mp
from DataStructures.Graph import edge as edg


def new_vertex(key, value):
    """
    Crea un nuevo vertice con la clave ``key`` y el valor ``value``.

    El vertice es creado con los siguientes atributos:

    - :attr:`key`: Llave del vertice.
    - :attr:`value`: Valor del vertice.
    - :attr:`adjacents`: Lista de :ref:`arcos<graph-edge>` adyacentes al vertice. Implementada como un mapa :ref:`map_linear_probing<map-linear-probing>`.

    :param key: Clave del vertice
    :type key: any
    :param value: Valor del vertice
    :type value: any

    :returns: Vertice recien creado
    :rtype: :ref:`vertex<graph-vertex>`

    .. include:: code-examples/Graph/vertex/new_vertex.rst

    """
    vertex = {"key": key, "value": value, "adjacents": mp.new_map(0, 0.5)}
    return vertex


def get_key(vertex):
    """
    Retorna el ``key`` del vertice ``vertex``.

    :param vertex: Vertice del cual se quiere obtener la clave
    :type vertex: :ref:`vertex<graph-vertex>`

    :returns: Clave del vertice
    :rtype: any

    .. include:: code-examples/Graph/vertex/get_key.rst
    """
    return vertex["key"]


def get_value(vertex):
    """
    Retorna el ``value`` del vertice ``vertex``

    :param vertex: Vertice del cual se quiere obtener el valor
    :type vertex: :ref:`vertex<graph-vertex>`

    :returns: Valor del vertice
    :rtype: any

    .. include:: code-examples/Graph/vertex/get_value.rst
    """
    return vertex["value"]


def set_value(vertex, new_value):
    """
    Cambia el ``value`` del vertice ``vertex`` por el valor ``new_value``

    :param vertex: Vertice al cual se le quiere cambiar el valor
    :type vertex: :ref:`vertex<graph-vertex>`
    :param value: Nuevo valor del vertice
    :type value: any

    .. include:: code-examples/Graph/vertex/set_value.rst
    """
    vertex["value"] = new_value


def get_adjacents(vertex):
    """
    Retorna el mapa con la lista de :ref:`arcos<graph-edge>`del vertice ``vertex``

    :param vertex: Vertice del cual se quiere obtener el mapa de arcos
    :type vertex: :ref:`vertex<graph-vertex>`

    :returns: Mapa de arcos del vertice
    :rtype: :ref:`map_linear_probing<map-linear-probing>`

    .. include:: code-examples/Graph/vertex/get_adjacents.rst
    """
    return vertex["adjacents"]


def get_edge(vertex, key_v):
    """
    Retorna el :ref:`arco<graph-edge>` adyacente en el vertice ``vertex`` con la clave ``key_v``
    Si no existe el arco, retorna None.

    :param vertex: Vertice del cual se quiere obtener el mapa de arcos
    :type vertex: :ref:`vertex<graph-vertex>`

    :returns: Mapa con la lista de arcos adyacentes vertice
    :rtype: :ref:`map_linear_probing<map-linear-probing>`

    .. include:: code-examples/Graph/vertex/get_edge.rst
    """
    return mp.get(vertex["adjacents"], key_v)


def add_adjacent(vertex, key_vertex, weight):
    """
    Agrega un :ref:`arco<graph-edge>` al vertice ``vertex`` con el vertice ``key_vertex`` y el peso ``weight``

    :param vertex: Vertice al cual se le quiere agregar el arco
    :type vertex: :ref:`vertex<graph-vertex>`
    :param key_vertex: Clave del vertice al cual se le quiere agregar el arco
    :type key_vertex: any
    :param weight: Peso del arco
    :type weight: double

    :returns: Vertice con el arco agregado
    :rtype: :ref:`vertex<graph-vertex>`

    .. include:: code-examples/Graph/vertex/add_adjacent.rst

    """
    new_edge = edg.new_edge(key_vertex, weight)
    vertex["adjacents"] = mp.put(vertex["adjacents"], key_vertex, new_edge)
    return vertex


def degree(vertex):
    """
    Retorna el grado del vertice ``vertex``.

    :param vertex: Vertice del cual se quiere obtener el grado
    :type vertex: :ref:`vertex<graph-vertex>`

    :returns: Grado del vertice
    :rtype: int

    .. include:: code-examples/Graph/vertex/degree.rst
    """
    return mp.size(vertex["adjacents"])
