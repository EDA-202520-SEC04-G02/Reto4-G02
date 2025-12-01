import pytest

from DataStructures.List import array_list as lt
un_ordered_list = [30, 50, 22, 10, 11, 13, 15, 14, 12, 17, 19, 18, 16, 20, 21]
ordered_list = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 30, 50]
reference_inverted_list = [50, 30, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10]

def setup_tests():
    empty_list = lt.new_list()
    one_element_list = lt.new_list()
    random_list = lt.new_list()
    inverted_list = lt.new_list()

    lt.add_first(one_element_list, 10)

    for i in range(0, 15):
        lt.add_last(random_list, un_ordered_list[i])

    for i in range(15,0,-1):
        lt.add_last(inverted_list, i)
    return empty_list, one_element_list, random_list, inverted_list

def sort_criteria_increasingly(element1, element2):
    is_sorted = False
    if element1 <= element2:
        is_sorted = True
    return is_sorted

def sort_criteria_decreasingly(element1, element2):
    is_sorted = False
    if element1 >= element2:
        is_sorted = True
    return is_sorted
    

@pytest.mark.skip(reason="No implementado aun")
def test_selection_sort():

    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.selection_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.selection_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1
    assert lt.first_element(one_element_list) == 10

    # Random list

    lt.selection_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15
    for i in range(0, 15):
        assert lt.get_element(random_lista, i) == ordered_list[i]

    # Inverted list

    lt.selection_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15
    for i in range(0, 15):
        assert lt.get_element(inverted_list, i) == i+1

    # Decreasingly sort criteria

    lt.selection_sort(back_up, sort_criteria_decreasingly)
    assert lt.size(back_up) == 15
    for i in range(0, 15):
        assert lt.get_element(back_up, i) == reference_inverted_list[i]

@pytest.mark.skip(reason="No implementado aun")
def test_insertion_sort():

    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.insertion_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.insertion_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1
    assert lt.first_element(one_element_list) == 10

    # Random list

    lt.insertion_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15
    for i in range(0, 14):
        assert lt.get_element(random_lista, i) == ordered_list[i]

    # Inverted list

    lt.insertion_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15
    for i in range(0, 14):
        assert lt.get_element(inverted_list, i) == i+1

    # Decreasingly sort criteria

    lt.insertion_sort(back_up, sort_criteria_decreasingly)
    assert lt.size(back_up) == 15
    for i in range(0, 14):
        assert lt.get_element(back_up, i) == reference_inverted_list[i]

@pytest.mark.skip(reason="No implementado aun")
def test_shell_sort():
    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.shell_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.shell_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1
    assert lt.first_element(one_element_list) == 10

    # Random list

    lt.shell_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15
    for i in range(0, 14):
        assert lt.get_element(random_lista, i) == ordered_list[i]

    # Inverted list

    lt.shell_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15
    for i in range(0, 14):
        assert lt.get_element(inverted_list, i) == i+1

    # Decreasingly sort criteria

    lt.shell_sort(back_up, sort_criteria_decreasingly)
    assert lt.size(back_up) == 15
    for i in range(0, 14):
        assert lt.get_element(back_up, i) == reference_inverted_list[i]

@pytest.mark.skip(reason="No implementado aun")
def test_merge_sort():
    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.merge_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.merge_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1
    assert lt.first_element(one_element_list) == 10

    # Random list

    lt.merge_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15
    for i in range(0, 14):
        assert lt.get_element(random_lista, i) == ordered_list[i]

    # Inverted list

    lt.merge_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15
    for i in range(0, 14):
        assert lt.get_element(inverted_list, i) == i+1

    # Decreasingly sort criteria

    lt.merge_sort(back_up, sort_criteria_decreasingly)
    assert lt.size(back_up) == 15
    for i in range(0, 14):
        assert lt.get_element(back_up, i) == reference_inverted_list[i]

@pytest.mark.skip(reason="No implementado aun")
def test_quick_sort():
    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.quick_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.quick_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1
    assert lt.first_element(one_element_list) == 10

    # Random list

    lt.quick_sort(random_lista, sort_criteria_increasingly)

    assert lt.size(random_lista) == 15
    for i in range(0, 14):
        assert lt.get_element(random_lista, i) == ordered_list[i]

    # Inverted list

    lt.quick_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15
    for i in range(0, 14):
        assert lt.get_element(inverted_list, i) == i+1

    # Decreasingly sort criteria

    lt.quick_sort(back_up, sort_criteria_decreasingly)
    assert lt.size(back_up) == 15
    for i in range(0, 14):
        assert lt.get_element(back_up, i) == reference_inverted_list[i]
