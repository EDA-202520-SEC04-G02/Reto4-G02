def new_edge(key_v, weight=0):
    """
    Crea un nuevo arco hacia el vertices con llave ``key_v`` con un peso ``weight``

    Se crea un arco con los siguientes atributos:

    - :attr:`to`: llave del vertice destino
    - :attr:`weight`: Peso del arco

    :param key_v: Llave del vertice destino del arco
    :type key_v: any
    :param weight: Peso del arco
    :type weight: double

    :returns: Arco recien creado
    :rtype: :ref:`edge<graph-edge>`

    .. include:: code-examples/Graph/edge/new_edge.rst
    """
    edge = {"to": key_v, "weight": weight}
    return edge


def to(edge):
    """
    Retorna la llave de vertice destino del arco ``edge``

    :param edge: Arco del cual se quiere obtener el vertice destino
    :type edge: :ref:`edge<graph-edge>`

    :returns: Llave del vertice destino del arco
    :rtype: any

    .. include:: code-examples/Graph/edge/to.rst
    """
    return edge["to"]


def weight(edge):
    """
    Retorna el peso del arco ``edge``

    :param edge: Arco del cual se quiere obtener el peso
    :type edge: :ref:`edge<graph-edge>`

    :returns: Peso del arco
    :rtype: double

    .. include:: code-examples/Graph/edge/weight.rst
    """
    return edge["weight"]


def set_weight(edge, new_weight):
    """
    Actualiza el peso del arco ``edge`` por el valor ``new_weight``

    :param edge: Arco al cual se le quiere cambiar el peso
    :type edge: :ref:`edge<graph-edge>`
    :param weight: Nuevo peso del arco
    :type weight: double

    :returns: Arco con el nuevo peso
    :rtype: :ref:`edge<graph-edge>`

    .. include:: code-examples/Graph/edge/set_weight.rst
    """
    edge["weight"] = new_weight
    return edge
