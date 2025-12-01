import pytest
from DataStructures.Utils.utils import handle_not_implemented
from DataStructures.Graph import digraph as G
from DataStructures.Map import map_linear_probing as mp
from DataStructures.List import array_list as lt
from DataStructures.Graph import edge as E
from DataStructures.Graph import vertex as V


def setup_tests():
    empty_graph = G.new_graph(0)
    some_graph = G.new_graph(2)

    vertex_1 = V.new_vertex(1, {"name": "A"})
    vertex_2 = V.new_vertex(2, {"name": "B"})

    V.add_adjacent(vertex_1, 2, 3.0)
    V.add_adjacent(vertex_2, 1, 3.0)

    vertex_1 = mp.put(some_graph["vertices"], 1, vertex_1)
    vertex_2 = mp.put(some_graph["vertices"], 2, vertex_2)
    some_graph["num_edges"] = 2

    return empty_graph, some_graph


@handle_not_implemented
def test_new_graph():
    graph = G.new_graph(10)
    assert graph["num_edges"] == 0
    assert graph["vertices"] is not None


@handle_not_implemented
def test_insert_vertex():
    empty_graph, some_graph = setup_tests()

    empty_graph = G.insert_vertex(empty_graph, 1, {"name": "A"})

    assert empty_graph["vertices"] is not None
    assert empty_graph["num_edges"] is not None
    assert type(empty_graph["num_edges"]) == int

    G.insert_vertex(some_graph, 3, {"name": "C"})
    assert some_graph["vertices"] is not None
    assert some_graph["num_edges"] is not None
    assert type(some_graph["num_edges"]) == int

    G.insert_vertex(some_graph, 1, {"name": "D"})
    assert some_graph["vertices"] is not None
    assert some_graph["num_edges"] is not None
    assert type(some_graph["num_edges"]) == int


@handle_not_implemented
def test_order():
    empty_graph, some_graph = setup_tests()

    assert G.order(empty_graph) == 0
    assert G.order(some_graph) == 2


@handle_not_implemented
def test_size():
    empty_graph, some_graph = setup_tests()

    assert G.size(empty_graph) == 0
    assert G.size(some_graph) == 2


@handle_not_implemented
def test_vertices():
    empty_graph, some_graph = setup_tests()

    vertices = G.vertices(empty_graph)

    assert lt.size(vertices) == 0

    vertices = G.vertices(some_graph)

    assert lt.size(vertices) == 2
    assert vertices["elements"] is not None


@handle_not_implemented
def test_degree():
    empty_graph, some_graph = setup_tests()

    try:
        G.degree(empty_graph, 1)
    except Exception as e:
        assert True

    try:
        G.degree(some_graph, 0)
    except Exception as e:
        assert True
    assert G.degree(some_graph, 1) == 1
    assert G.degree(some_graph, 2) == 1


@handle_not_implemented
def test_add_edge():
    empty_graph, some_graph = setup_tests()

    try:
        G.add_edge(empty_graph, 1, 2, 3.0)
    except Exception as e:
        assert True

    G.insert_vertex(some_graph, 3, {"name": "D"})
    G.add_edge(some_graph, 1, 3, 3.0)

    assert G.size(some_graph) == 3
