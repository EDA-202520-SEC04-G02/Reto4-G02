from DataStructures.List import array_list as lt
from DataStructures.Utils.utils import handle_not_implemented

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
    if element1 < element2:
        is_sorted = True
    return is_sorted

@handle_not_implemented    
def test_default_sort_criteria():
    assert lt.default_sort_criteria(1, 2) == True 
    assert lt.default_sort_criteria(2, 1) == False 

@handle_not_implemented
def test_selection_sort():

    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.selection_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.selection_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1

    # Random list

    lt.selection_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15

    # Inverted list

    lt.selection_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15


@handle_not_implemented
def test_insertion_sort():

    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.insertion_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.insertion_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1

    # Random list

    lt.insertion_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15

    # Inverted list

    lt.insertion_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15



@handle_not_implemented
def test_shell_sort():
    empty_list, one_element_list, random_lista, inverted_list = setup_tests()
    back_up = random_lista

    # Empty list

    lt.shell_sort(empty_list, sort_criteria_increasingly)
    assert lt.size(empty_list) == 0

    # One element list

    lt.shell_sort(one_element_list, sort_criteria_increasingly)
    assert lt.size(one_element_list) == 1

    # Random list

    lt.shell_sort(random_lista, sort_criteria_increasingly)
    assert lt.size(random_lista) == 15

    # Inverted list

    lt.shell_sort(inverted_list, sort_criteria_increasingly)
    assert lt.size(inverted_list) == 15
