"""
  Estructura que contiene la información a guardar en una ``entry`` de un Map
"""


def new_map_entry(key, value):
    """
    Crea una nueva entrada (de tipo :ref:`map_entry<map-entry>`) de una tabla con una llave y un valor dados.

    La entrada es creada con los siguientes atributos:

    * **key**: Llave de la entrada. Inicializada con el valor de la llave dada ``key``.
    * **value**: Valor de la entrada. Inicializada con el valor del valor dado ``value``.

    :param key: Llave de la entrada.
    :type key: any
    :param value: Valor de la entrada.
    :type value: any

    :return: Entrada de una tabla.
    :rtype: :ref:`map_entry<map-entry>`
    """
    entry = {"key": key, "value": value}
    return entry


def set_key(my_entry, key):
    """
    Establece un valor nuevo a la ``key`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`map_entry<map-entry>`
    :param key: Llave nueva de la entrada.
    :type key: any

    :return: Entrada con la llave modificada.
    :rtype: :ref:`map_entry<map-entry>`
    """
    my_entry["key"] = key
    return my_entry


def set_value(my_entry, value):
    """
    Establece un valor nuevo al ``value`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`map_entry<map-entry>`
    :param value: Valor nuevo de la entrada.
    :type value: any

    :return: Entrada con el valor modificado.
    :rtype: :ref:`map_entry<map-entry>`
    """
    my_entry["value"] = value
    return my_entry


def get_key(my_entry):
    """
    Obtiene la llave ``key`` de una entrada recibida.

    :param my_entry: Entrada de la cual se desea obtener la llave.
    :type my_entry: :ref:`map_entry<map-entry>`

    :return: Llave de la entrada.
    :rtype: any
    """
    return my_entry["key"]


def get_value(my_entry):
    """
    Obtiene el valor ``value`` de una entrada recibida.

    :param my_entry: Entrada de la cual se desea obtener el valor.
    :type my_entry: :ref:`map_entry<map-entry>`

    :return: Valor de la entrada.
    :rtype: any
    """
    return my_entry["value"]
