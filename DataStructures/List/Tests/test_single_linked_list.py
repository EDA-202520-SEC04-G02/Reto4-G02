from DataStructures.List import single_linked_list as lt
from DataStructures.Utils.utils import handle_not_implemented


def setup_tests():
    return lt.new_list()


def compare_from_tests(element1, element2):
    if element1 == element2:
        return 0
    elif element1 > element2:
        return 1
    return -1


@handle_not_implemented
def test_new_list():
    lista = setup_tests()

    assert type(lista) == dict
    assert lista["first"] is None
    assert lista["last"] is None
    assert lista["size"] == 0


@handle_not_implemented
def test_add_first():
    lista = setup_tests()

    lt.add_first(lista, 1)

    assert type(lista) == dict
    assert type(lista["first"]) == dict
    assert type(lista["last"]) == dict


@handle_not_implemented
def test_add_last():
    lista = setup_tests()

    lt.add_last(lista, 1)

    assert type(lista) == dict
    assert type(lista["first"]) == dict
    assert type(lista["last"]) == dict


@handle_not_implemented
def test_is_empty():
    lista = setup_tests()

    assert lt.is_empty(lista) is not None
    assert type(lt.is_empty(lista)) == bool


@handle_not_implemented
def test_get_size():
    lista = setup_tests()

    assert lt.size(lista) is not None
    assert type(lt.size(lista)) == int


@handle_not_implemented
def test_get_first_element():

    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lista) == dict
    assert lt.first_element(lista) is not None


@handle_not_implemented
def test_get_last_element():

    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lista) == dict
    assert lt.last_element(lista) is not None


@handle_not_implemented
def test_get_element():

    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lista) == dict
    assert lt.get_element(lista, 0) is not None


@handle_not_implemented
def test_remove_first():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lista) == dict
    assert type(lt.remove_first(lista)) == int


@handle_not_implemented
def test_remove_last():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lista) == dict
    assert type(lt.remove_last(lista)) == int


@handle_not_implemented
def test_insert_element():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lista) == dict
    assert type(lt.insert_element(lista, 2, 0)) == dict


@handle_not_implemented
def test_is_present():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lt.is_present(lista, 1, compare_from_tests)) is int


@handle_not_implemented
def test_delete_element():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert type(lt.delete_element(lista, 0)) == dict


@handle_not_implemented
def test_change_info():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    assert lt.change_info(lista, 0, 4) is not None


@handle_not_implemented
def test_exchange():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": {
        "info": 2, "next": {"info": 3, "next": None}}}
    lista["last"] = {"info": 3, "next": None}

    assert lt.exchange(lista, 0, 0) is not None


@handle_not_implemented
def test_sub_list():
    lista = setup_tests()

    lista["size"] = 1
    lista["first"] = {"info": 1, "next": None}
    lista["last"] = lista["first"]

    sub_list = lt.sub_list(lista, 0, 0)

    assert sub_list is not None
    assert type(sub_list) == dict
