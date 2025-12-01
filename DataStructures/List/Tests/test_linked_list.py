import pytest
from DataStructures.List import single_linked_list as lt


def setup_tests():
    return lt.new_list()

def compare_from_tests(element1, element2):
    if element1 == element2:
        return 0
    elif element1 > element2:
        return 1
    return -1

@pytest.mark.skip(reason="No implementado aun")
def test_new_list():
    lista = setup_tests()
    assert lista["first"] is None
    assert lista["last"] is None
    assert lista["size"] == 0


@pytest.mark.skip(reason="No implementado aun")
def test_add_first():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    assert lista["size"] == 3
    assert lista["first"]["info"] == 3


@pytest.mark.skip(reason="No implementado aun")
def test_add_last():
    lista = setup_tests()

    lt.add_last(lista, 1)
    lt.add_last(lista, 2)
    lt.add_last(lista, 3)

    assert lista["size"] == 3
    assert lista["first"]["info"] == 1
    assert lista["first"]["next"]["info"] == 2
    assert lista["last"]["info"] == 3


@pytest.mark.skip(reason="No implementado aun")
def test_is_empty():
    lista = setup_tests()

    assert lt.is_empty(lista) == True

    lt.add_first(lista, 1)
    assert lt.is_empty(lista) == False


@pytest.mark.skip(reason="No implementado aun")
def test_get_size():
    lista = setup_tests()

    assert lt.size(lista) == 0

    lista["size"] = 1

    assert lt.size(lista) == 1


@pytest.mark.skip(reason="No implementado aun")
def test_get_first_element():

    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)
    assert lt.first_element(lista) == 3


@pytest.mark.skip(reason="No implementado aun")
def test_get_last_element():

    lista = setup_tests()

    lt.add_first(lista, 1)

    lt.add_first(lista, 2)
    lt.add_first(lista, 3)
    assert lt.last_element(lista) == 1


@pytest.mark.skip(reason="No implementado aun")
def test_get_element():

    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    assert lt.get_element(lista, 0) == 3
    assert lt.get_element(lista, 1) == 2
    assert lt.get_element(lista, 2) == 1


@pytest.mark.skip(reason="No implementado aun")
def test_remove_first():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    lt.remove_first(lista)

    assert lista["size"] == 2
    assert lista["first"]["info"] == 2
    assert lista["last"]["info"] == 1


@pytest.mark.skip(reason="No implementado aun")
def test_remove_last():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    lt.remove_last(lista)

    assert lista["size"] == 2
    assert lista["first"]["info"] == 3
    assert lista["last"]["info"] == 2


@pytest.mark.skip(reason="No implementado aun")
def test_insert_element():
    lista = setup_tests()

    lt.insert_element(lista, 1, 0)

    assert lista["size"] == 1
    assert lista["first"]["info"] == 1
    assert lista["last"]["info"] == 1

    lt.insert_element(lista, 2, 0)

    assert lista["size"] == 2
    assert lista["first"]["info"] == 2
    assert lista["last"]["info"] == 1

    lt.insert_element(lista, 3, 1)

    assert lista["size"] == 3
    assert lista["first"]["info"] == 2
    assert lista["first"]["next"]["info"] == 3
    assert lista["last"]["info"] == 1

    lt.insert_element(lista, 4, 3)

    assert lista["size"] == 4
    assert lista["first"]["info"] == 2
    assert lista["first"]["next"]["info"] == 3
    assert lista["first"]["next"]["next"]["info"] == 1
    assert lista["first"]["next"]["next"]["next"]["info"] == 4
    assert lista["last"]["info"] == 4

@pytest.mark.skip(reason="No implementado aun")
def test_is_present():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    assert lt.is_present(lista, 1, compare_from_tests) == 2
    assert lt.is_present(lista, 2, compare_from_tests) == 1
    assert lt.is_present(lista, 3, compare_from_tests) == 0
    assert lt.is_present(lista, 4, compare_from_tests) == -1


@pytest.mark.skip(reason="No implementado aun")
def test_delete_element():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    lt.delete_element(lista, 2)

    assert lista["size"] == 2
    assert lista["first"]["info"] == 3
    assert lista["last"]["info"] == 2


@pytest.mark.skip(reason="No implementado aun")
def test_change_info():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    lt.change_info(lista, 2, 4)

    assert lista["size"] == 3
    assert lista["first"]["info"] == 3
    assert lista["first"]["next"]["info"] == 2
    assert lista["last"]["info"] == 4


@pytest.mark.skip(reason="No implementado aun")
def test_exchange():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    lt.exchange(lista, 0, 2)

    assert lista["size"] == 3
    assert lista["first"]["info"] == 1
    assert lista["first"]["next"]["info"] == 2
    assert lista["first"]["next"]["next"]["info"] == 3
    assert lista["last"]["info"] == 3


@pytest.mark.skip(reason="No implementado aun")
def test_sub_list():
    lista = setup_tests()

    lt.add_first(lista, 1)
    lt.add_first(lista, 2)
    lt.add_first(lista, 3)

    sub_list = lt.sub_list(lista, 0, 2)

    assert sub_list["size"] == 2
    assert sub_list["first"]["info"] == 3
    assert sub_list["last"]["info"] == 2

