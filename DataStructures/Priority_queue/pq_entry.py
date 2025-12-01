def new_pq_entry(priority, value):
    """
    Crea una nueva entrada (de tipo :ref:`pq_entry<priority-queue-entry>`) de una cola de prioridad.

    La entrada es creada con los siguientes atributos:

    - :attr:`priority`: Prioridad de la entrada. Inicializada con el valor de la prioridad dada ``priority``.
    - :attr:`value`: Valor de la entrada. Inicializada con el valor del valor dado ``value``.

    :param priority: Prioridad de la entrada.
    :type priority: any
    :param value: Valor de la entrada.
    :type value: any

    :return: Entrada de una cola de prioridad.
    :rtype: :ref:`pq_entry<priority-queue-entry>`
    """
    return {
        "priority": priority,
        "value": value,
    }

def set_priority(my_entry, priority):
    """
    Establece un valor nuevo a la ``priority`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`pq_entry<priority-queue-entry>`
    :param priority: Prioridad nueva de la entrada.
    :type priority: any

    :return: Entrada con la prioridad modificada.
    :rtype: :ref:`pq_entry<priority-queue-entry>`
    """
    my_entry["priority"] = priority
    return my_entry

def set_value(my_entry, value):
    """
    Establece un valor nuevo al ``value`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`pq_entry<priority-queue-entry>`
    :param value: Valor nuevo de la entrada.
    :type value: any

    :return: Entrada con el valor modificado.
    :rtype: :ref:`pq_entry<priority-queue-entry>`
    """
    my_entry["value"] = value
    return my_entry

def get_priority(my_entry):
    """
    Obtiene la prioridad ``priority`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`pq_entry<priority-queue-entry>`

    :return: Prioridad de la entrada.
    :rtype: any
    """
    return my_entry["priority"]

def get_value(my_entry):
    """
    Obtiene el valor ``value`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`pq_entry<priority-queue-entry>`

    :return: Valor de la entrada.
    :rtype: any
    """
    return my_entry["value"]