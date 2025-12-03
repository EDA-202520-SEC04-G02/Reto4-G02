# ----------------------------------------------------
import time, csv
from datetime import datetime
import math
csv.field_size_limit(2147483647)
from DataStructures.List import array_list as lt
from DataStructures.List import single_linked_list as sl
from DataStructures.Map import map_linear_probing as mp 
from DataStructures.Stack import stack as st
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Graph import digraph as G
from DataStructures.Graph import dfs as DFS
from DataStructures.Graph import bfs as BFS
from DataStructures.Graph import dijsktra_structure as DIJ
# ----------------------------------------------------
# Catalogo de datos
# ----------------------------------------------------
def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO DONE: Llama a las funciónes de creación de las estructuras de datos
    catalog = {
        # Grafo 1: peso = distancia promedio de desplazamiento entre puntos migratorios
        "graph_dist": G.new_graph(1000),

        # Grafo 2: peso = distancia promedio a la fuente hídrica del destino
        "graph_agua": G.new_graph(1000),

        # tag-local-identifier -> lista de eventos de esa grulla
        "events_by_tag": mp.new_map(1000, 0.5),

        # event-id -> id del vértice (punto migratorio) al que pertenece
        "event_to_vertex": mp.new_map(1000, 0.5),

        # vertex-id -> información del vértice (lat, lon, tiempo, tags, eventos, etc.)
        "vertices_info": mp.new_map(1000, 0.5),

        # orden de creación de los vértices (para imprimir primeros/últimos)
        "vertices_order": lt.new_list()
    }
    return catalog
# ----------------------------------------------------
# Funciones auxiliares
# ----------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    """
    Calcula la distancia Haversine en km entre dos puntos.
    """
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def cmp_event_time(e1, e2):
    """
    Criterio de ordenamiento: evento e1 va antes que e2
    si su timestamp es más antiguo.
    """
    return e1["time"] < e2["time"]

def list_contains_str(str_list, value):
    """
    Revisa si un string está en una lista tipo array_list.
    """
    size = lt.size(str_list)
    for i in range(size):
        if lt.get_element(str_list, i) == value:
            return True
    return False

# ----------------------------------------------------
# Funciones para la carga de datos
# ----------------------------------------------------

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO DONE: Realizar la carga de datos
    
    start_time = time.perf_counter()

    start = time.perf_counter()

    events_by_tag = catalog["events_by_tag"]

    # Lista global de todos los eventos (para crear vértices)
    all_events = lt.new_list()

    with open(filename, encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            event = {
                "event_id": row["event-id"],
                "tag": row["tag-local-identifier"],
                "lat": float(row["location-lat"]),
                "lon": float(row["location-long"]),
                # Todos los timestamps vienen en formato "%Y-%m-%d %H:%M:%S.%f"
                "time": datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S.%f"),
                # comments viene en metros -> convertir a km
                "dist_agua_km": float(row["comments"]) / 1000.0
            }

            lt.add_last(all_events, event)

            # Agregar el evento a la lista de su tag
            tag_events = mp.get(events_by_tag, event["tag"])
            if tag_events is None:
                tag_events = lt.new_list()
                mp.put(events_by_tag, event["tag"], tag_events)
            lt.add_last(tag_events, event)

    # Construir vértices (puntos migratorios)
    build_vertices(catalog, all_events)

    # Construir arcos de los dos grafos
    build_edges(catalog)

    elapsed = time.perf_counter() - start
    return elapsed

# ----------------------------------------------------
# Construcción de vértices
# ----------------------------------------------------

def find_vertex_for_event(vertices_info, vertices_order, event):
    """
    Busca un vértice existente en el que el evento pueda encajar
    cumpliendo:
      - distancia Haversine < 3 km
      - diferencia de tiempo absoluta < 3 h

    Retorna el id del vértice, o None si no se encuentra.
    """
    num_vertices = lt.size(vertices_order)
    for i in range(num_vertices):
        v_id = lt.get_element(vertices_order, i)
        v = mp.get(vertices_info, v_id)

        dist_km = haversine(event["lat"], event["lon"], v["lat"], v["lon"])
        if dist_km >= 3.0:
            continue

        hours = abs((event["time"] - v["creation_time"]).total_seconds()) / 3600.0
        if hours >= 3.0:
            continue

        return v_id

    return None


def create_vertex_for_event(catalog, event):
    """
    Crea un nuevo vértice a partir de un evento que no encaja en
    ningún vértice existente.
    """
    vertices_info = catalog["vertices_info"]
    vertices_order = catalog["vertices_order"]

    vertex_id = event["event_id"]

    vertex = {
        "id": vertex_id,
        "lat": event["lat"],
        "lon": event["lon"],
        "creation_time": event["time"],
        "tags": lt.new_list(),    # lista de tag-local-identifier
        "events": lt.new_list(),  # lista de event-id
        "sum_agua": event["dist_agua_km"],
        "count": 1,
        "avg_agua": event["dist_agua_km"]
    }

    lt.add_last(vertex["tags"], event["tag"])
    lt.add_last(vertex["events"], event["event_id"])

    mp.put(vertices_info, vertex_id, vertex)
    lt.add_last(vertices_order, vertex_id)

    # Insertar vértice en ambos grafos
    G.insert_vertex(catalog["graph_dist"], vertex_id, None)
    G.insert_vertex(catalog["graph_agua"], vertex_id, None)

    return vertex_id


def update_vertex_with_event(vertices_info, vertex_id, event):
    """
    Actualiza un vértice existente con un nuevo evento que le pertenece:
    - Agrega el tag si no estaba
    - Agrega el event-id
    - Actualiza promedio de distancia al agua
    """
    vertex = mp.get(vertices_info, vertex_id)

    # Actualizar tags
    if not list_contains_str(vertex["tags"], event["tag"]):
        lt.add_last(vertex["tags"], event["tag"])

    # Actualizar lista de eventos
    lt.add_last(vertex["events"], event["event_id"])

    # Actualizar promedio de distancia al agua
    vertex["sum_agua"] += event["dist_agua_km"]
    vertex["count"] += 1
    vertex["avg_agua"] = vertex["sum_agua"] / vertex["count"]

    # No es estrictamente necesario hacer put de nuevo porque modificamos
    # el diccionario por referencia, pero no hace daño si se quiere.
    mp.put(vertices_info, vertex_id, vertex)


def build_vertices(catalog, all_events):
    """
    Agrupa los eventos en Puntos Migratorios (vértices) siguiendo las reglas:
      - Ordenar todos los eventos por timestamp (global).
      - Un evento pertenece a un vértice si:
          * Está a menos de 3 km del vértice.
          * Está a menos de 3 horas del tiempo de creación del vértice.
      - El id del vértice es el event-id del primer evento asignado.
      - Cada vértice guarda:
          * posición (lat, lon) del primer evento
          * creation_time
          * lista de tags (grullas)
          * lista de event-id
          * count de eventos
          * promedio de distancia al agua (km)
    """
    vertices_info = catalog["vertices_info"]
    vertices_order = catalog["vertices_order"]
    event_to_vertex = catalog["event_to_vertex"]

    # Ordenar todos los eventos por tiempo
    all_events = lt.merge_sort(all_events, cmp_event_time)

    num_events = lt.size(all_events)
    for i in range(num_events):
        event = lt.get_element(all_events, i)

        # 1. Buscar si existe un vértice compatible
        vertex_id = find_vertex_for_event(vertices_info, vertices_order, event)

        # 2. Si no existe, crear un nuevo vértice
        if vertex_id is None:
            vertex_id = create_vertex_for_event(catalog, event)
        else:
            # 3. Si existe, actualizar el vértice con este evento
            update_vertex_with_event(vertices_info, vertex_id, event)

        # 4. Registrar el mapeo evento -> vértice
        mp.put(event_to_vertex, event["event_id"], vertex_id)


# ----------------------------------------------------
# Construcción de arcos
# ----------------------------------------------------

def build_edges(catalog):
    """
    Recorre los eventos de cada grulla (tag-local-identifier) en orden
    temporal, detecta viajes A->B entre puntos migratorios y construye
    los arcos de los dos grafos.

    Para cada par (A, B) se acumula:
      - sum_dist: suma de distancias Haversine entre A y B
      - sum_agua: suma de avg_agua(B) de los viajes A->B
      - count: número de viajes A->B

    Al final se agrega un solo arco A->B en cada grafo con el promedio
    correspondiente.
    """
    events_by_tag = catalog["events_by_tag"]
    vertices_info = catalog["vertices_info"]
    event_to_vertex = catalog["event_to_vertex"]
    g_dist = catalog["graph_dist"]
    g_agua = catalog["graph_agua"]

    # Mapa local: (A, B) -> {"sum_dist": ..., "sum_agua": ..., "count": ...}
    edge_stats = mp.new_map(1000, 0.5)

    # Recorrer todos los tags
    tag_keys = mp.key_set(events_by_tag)
    num_tags = lt.size(tag_keys)

    for i in range(num_tags):
        tag = lt.get_element(tag_keys, i)
        events = mp.get(events_by_tag, tag)

        # Ordenar eventos de esta grulla por tiempo
        events = lt.merge_sort(events, cmp_event_time)
        mp.put(events_by_tag, tag, events)

        prev_vertex = None
        num_events = lt.size(events)

        for j in range(num_events):
            event = lt.get_element(events, j)
            curr_vertex = mp.get(event_to_vertex, event["event_id"])

            if prev_vertex is None:
                prev_vertex = curr_vertex
                continue

            # Si sigue en el mismo vértice, no hay viaje
            if curr_vertex == prev_vertex:
                continue

            # Hay un viaje A -> B
            A = prev_vertex
            B = curr_vertex

            vA = mp.get(vertices_info, A)
            vB = mp.get(vertices_info, B)

            dist_km = haversine(vA["lat"], vA["lon"], vB["lat"], vB["lon"])
            agua_B = vB["avg_agua"]

            key = (A, B)
            stats = mp.get(edge_stats, key)
            if stats is None:
                stats = {
                    "sum_dist": dist_km,
                    "sum_agua": agua_B,
                    "count": 1
                }
            else:
                stats["sum_dist"] += dist_km
                stats["sum_agua"] += agua_B
                stats["count"] += 1

            mp.put(edge_stats, key, stats)

            prev_vertex = curr_vertex

    # Crear arcos a partir de los promedios acumulados
    edge_keys = mp.key_set(edge_stats)
    num_edges = lt.size(edge_keys)

    for i in range(num_edges):
        key = lt.get_element(edge_keys, i)
        stats = mp.get(edge_stats, key)
        A, B = key

        avg_dist = stats["sum_dist"] / stats["count"]
        avg_agua = stats["sum_agua"] / stats["count"]

        G.add_edge(g_dist, A, B, avg_dist)
        G.add_edge(g_agua, A, B, avg_agua)

# ----------------------------------------------------
# Utilidad para la vista: muestras de vértices
# ----------------------------------------------------

def tags_to_string(tags_list):
    """
    Convierte la lista de tags (TDA de lista) en un string del tipo:
    'cantidad: [tag1, tag2, ...]'.
    """
    size = lt.size(tags_list)
    tags_py = []
    for i in range(size):
        tag = lt.get_element(tags_list, i)
        tags_py.append(str(tag))
    tags_repr = "[" + ", ".join(tags_py) + "]"
    return f"{size}: {tags_repr}"


def get_vertices_samples(catalog, n=5):
    """
    Retorna dos listas de diccionarios con la información de los primeros
    y últimos n vértices para impresión en la vista.

    Columnas:
    - Identificador único
    - Posición (lat, lon)
    - Fecha de creación
    - Grullas (tags)           -> 'cantidad: [lista de tags]'
    - Conteo de eventos
    - Dist. Hídrica Prom (km)
    """
    vertices_info = catalog["vertices_info"]
    order = catalog["vertices_order"]

    total = lt.size(order)
    if total == 0:
        return [], []

    n = min(n, total)

    def vertex_to_dict(v):
        return {
            "Identificador único": v["id"],
            "Posición (lat, lon)": f"({v['lat']}, {v['lon']})",
            "Fecha de creación": v["creation_time"],
            "Grullas (tags)": tags_to_string(v["tags"]),
            "Conteo de eventos": v["count"],
            "Dist. Hídrica Prom (km)": round(v["avg_agua"], 4)
        }

    first, last = [], []

    # Primeros n
    for i in range(n):
        v = mp.get(vertices_info, lt.get_element(order, i))
        first.append(vertex_to_dict(v))

    # Últimos n
    for i in range(total - n, total):
        v = mp.get(vertices_info, lt.get_element(order, i))
        last.append(vertex_to_dict(v))

    return first, last

# ----------------------------------------------------
# Funciones de consulta sobre el catálogo
# ----------------------------------------------------

def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
