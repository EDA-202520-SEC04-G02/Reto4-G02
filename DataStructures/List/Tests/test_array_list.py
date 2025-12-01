from DataStructures.List import array_list as lt
from DataStructures.Utils.utils import handle_not_implemented


def setup_tests():
    # Función que inicializa la lista para las pruebas
    # Retorna una lista vacía
    return lt.new_list()


def compare_from_tests(element1, element2):
    # Función que compara dos elementos para las pruebas
    # Retorna 0 si son iguales, 1 si el primer elemento es mayor
    if element1 == element2:
        return 0
    elif element1 > element2:
        return 1
    return -1


@handle_not_implemented
def test_new_list():
    # Este teste verifica que la lista se crea correctamente
    # y que no tiene elementos
    lista = setup_tests()

    assert type(lista) == dict
    assert lista["size"] == 0
    assert lista["elements"] == []


@handle_not_implemented
def test_add_first():
    # Este test verifica que se añaden elementos
    # Valida que el tamaño de la lista sea correcto
    lista = setup_tests()

    lt.add_first(lista, 1)

    assert type(lista) == dict
    assert type(lista["size"]) == int


@handle_not_implemented
def test_add_last():
    lista = setup_tests()

    lt.add_last(lista, 1)

    assert type(lista) == dict
    assert type(lista["size"]) == int


@handle_not_implemented
def test_is_empty():
    lista = setup_tests()

    assert lt.is_empty(lista) is not None
    assert type(lt.is_empty(lista)) == bool

    lista["size"] = 1
    lista["elements"] = [1]

    assert lt.is_empty(lista) is not None
    assert type(lt.is_empty(lista)) == bool


@handle_not_implemented
def test_get_size():
    lista = setup_tests()

    assert lt.size(lista) is not None
    assert type(lt.size(lista)) == int

    lista["size"] = 1
    lista["elements"] = [1]

    assert lt.size(lista) is not None
    assert type(lt.size(lista)) == int


@handle_not_implemented
def test_get_first_element():

    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [1, 2, 3]

    assert lt.first_element(lista) is not None
    assert type(lt.first_element(lista)) == int


@handle_not_implemented
def test_get_last_element():

    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [1, 2, 3]

    assert lt.first_element(lista) is not None
    assert type(lt.first_element(lista)) == int


@handle_not_implemented
def test_get_element():

    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [1, 2, 3]

    assert type(lista) == dict
    assert lt.get_element(lista, 0) is not None


@handle_not_implemented
def test_remove_first():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [1, 2, 3]

    assert lt.remove_first(lista) is not None
    assert type(lt.remove_first(lista)) == int


@handle_not_implemented
def test_remove_last():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [1, 2, 3]

    assert lt.remove_last(lista) is not None
    assert type(lt.remove_last(lista)) == int


@handle_not_implemented
def test_insert_element():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [1, 2, 3]

    assert lt.insert_element(lista, 2, 3) is not None
    assert type(lt.insert_element(lista, 2, 3)) == dict


@handle_not_implemented
def test_is_present():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [3, 2, 1]

    assert type(lt.is_present(lista, 1, compare_from_tests)) is int


@handle_not_implemented
def test_delete_element():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [3, 2, 1]

    assert lt.delete_element(lista, 1) is not None
    assert type(lt.delete_element(lista, 1)) == dict


@handle_not_implemented
def test_change_info():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [3, 2, 1]

    assert lt.change_info(lista, 1, 4) is not None


@handle_not_implemented
def test_exchange():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [3, 2, 1]

    assert lt.exchange(lista, 0, 1) is not None


@handle_not_implemented
def test_sublist():
    lista = setup_tests()

    lista["size"] = 3
    lista["elements"] = [3, 2, 1]

    sub_list = lt.sub_list(lista, 0, 2)

    assert sub_list is not None
    assert type(sub_list) == dict
