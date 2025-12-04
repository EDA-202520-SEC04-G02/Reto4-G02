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
        "graph_dist": G.new_graph(23000),

        # Grafo 2: peso = distancia promedio a la fuente hídrica del destino
        "graph_agua": G.new_graph(23000),

        # tag-local-identifier -> lista de eventos de esa grulla
        "events_by_tag": mp.new_map(23000, 0.5),

        # event-id -> id del vértice (punto migratorio) al que pertenece
        "event_to_vertex": mp.new_map(23000, 0.5),

        # vertex-id -> información del vértice (lat, lon, tiempo, tags, eventos, etc.)
        "vertices_info": mp.new_map(23000, 0.5),

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

def find_closest_vertex(catalog, lat, lon):
    """
    Retorna el id del vértice más cercano a la coordenada (lat, lon)
    y la distancia correspondiente en km.
    """
    vertices_info = catalog["vertices_info"]
    order = catalog["vertices_order"]

    best_id = None
    best_dist = None

    size = lt.size(order)
    for i in range(size):
        v_id = lt.get_element(order, i)
        v = mp.get(vertices_info, v_id)

        d = haversine(lat, lon, v["lat"], v["lon"])
        if best_dist is None or d < best_dist:
            best_dist = d
            best_id = v_id

    return best_id, best_dist

def find_closest_vertex_on_route(route_vertices, vertices_info, lat, lon):
    """
    Retorna el vértice de route_vertices más cercano a (lat, lon)
    junto con su índice en la ruta y la distancia Haversine.

    route_vertices: lista TDA (array_list) con ids de vértice.
    """
    size = lt.size(route_vertices)
    best_idx = None
    best_id = None
    best_dist = None

    for i in range(size):
        v_id = lt.get_element(route_vertices, i)
        v = mp.get(vertices_info, v_id)
        if v is None:
            continue

        d = haversine(lat, lon, v["lat"], v["lon"])
        if best_dist is None or d < best_dist:
            best_dist = d
            best_id = v_id
            best_idx = i

    return best_id, best_idx, best_dist


# ----------------------------------------------------
# Funciones para la carga de datos
# ----------------------------------------------------

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO DONE: Realizar la carga de datos
    
    start = get_time()

    events_by_tag = catalog["events_by_tag"]

    # Lista global de todos los eventos (para crear vértices y arcos)
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

            # Agregar a lista global
            lt.add_last(all_events, event)
            
    # Ordenar globalmente todos los eventos por tiempo (UNA sola vez)
    all_events = lt.merge_sort(all_events, cmp_event_time)

    # Ahora sí, distribuir los eventos en events_by_tag
    num_events = lt.size(all_events)
    for i in range(num_events):
        e = lt.get_element(all_events, i)
        tag = e["tag"]
        tag_events = mp.get(events_by_tag, tag)
        if tag_events is None:
            tag_events = lt.new_list()
            mp.put(events_by_tag, tag, tag_events)

        # IMPORTANTE: agregar el evento a la lista del tag
        lt.add_last(tag_events, e)

    # Construir vértices (puntos migratorios) usando la lista ya ordenada
    build_vertices(catalog, all_events)

    # Construir arcos de los dos grafos usando la misma lista ordenada
    build_edges(catalog, all_events)

    end = get_time()
    delta = delta_time(start, end)
    return delta

# ----------------------------------------------------
# Construcción de vértices
# ----------------------------------------------------

def find_vertex_for_event_window(vertices_info, vertices_order, first_active_idx, event):
    num_vertices = lt.size(vertices_order)

    for i in range(first_active_idx, num_vertices):
        v_id = lt.get_element(vertices_order, i)
        v = mp.get(vertices_info, v_id)

        dist_km = haversine(event["lat"], event["lon"], v["lat"], v["lon"])
        if dist_km < 3.0:
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

    Optimización:
      - Se mantiene un índice first_active_idx que apunta al primer vértice
        cuyo creation_time está a menos de 3 h del evento actual.
      - Solo se buscan candidatos entre los vértices en la ventana
        [first_active_idx, num_vertices).
    """
    vertices_info = catalog["vertices_info"]
    vertices_order = catalog["vertices_order"]
    event_to_vertex = catalog["event_to_vertex"]

    num_events = lt.size(all_events)
    first_active_idx = 0  # índice del primer vértice potencialmente activo

    for i in range(num_events):
        event = lt.get_element(all_events, i)

        # Actualizar ventana temporal de vértices activos.
        # Avanzamos first_active_idx mientras el vértice tenga
        # creation_time más de 3 h en el pasado respecto al evento actual.
        num_vertices = lt.size(vertices_order)
        while first_active_idx < num_vertices:
            v_id = lt.get_element(vertices_order, first_active_idx)
            v = mp.get(vertices_info, v_id)

            hours = (event["time"] - v["creation_time"]).total_seconds() / 3600.0
            if hours >= 3.0:
                # Este vértice ya no puede recibir eventos futuros
                first_active_idx += 1
            else:
                # A partir de aquí los vértices son lo bastante recientes
                break

        # Buscar vértice compatible solo en la ventana [first_active_idx, num_vertices)
        vertex_id = find_vertex_for_event_window(
            vertices_info,
            vertices_order,
            first_active_idx,
            event
        )

        # Si no existe, crear un nuevo vértice
        if vertex_id is None:
            vertex_id = create_vertex_for_event(catalog, event)
        else:
            # Si existe, actualizarlo con este evento
            update_vertex_with_event(vertices_info, vertex_id, event)

        # Registrar el mapeo evento -> vértice
        mp.put(event_to_vertex, event["event_id"], vertex_id)



# ----------------------------------------------------
# Construcción de arcos
# ----------------------------------------------------

def build_edges(catalog, all_events):
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
    vertices_info   = catalog["vertices_info"]
    event_to_vertex = catalog["event_to_vertex"]
    g_dist          = catalog["graph_dist"]
    g_agua          = catalog["graph_agua"]

    # Mapa local: (A, B) -> {"sum_dist": ..., "sum_agua": ..., "count": ...}
    edge_stats = mp.new_map(23000, 0.5)

    # Mapa tag -> último vértice visitado por esa grulla
    last_vertex_by_tag = mp.new_map(23000, 0.5)

    num_events = lt.size(all_events)
    for i in range(num_events):
        e = lt.get_element(all_events, i)
        tag   = e["tag"]
        ev_id = e["event_id"]

        curr_vertex = mp.get(event_to_vertex, ev_id)
        if curr_vertex is None:
            continue

        prev_vertex = mp.get(last_vertex_by_tag, tag)

        # Primer evento de este tag
        if prev_vertex is None:
            mp.put(last_vertex_by_tag, tag, curr_vertex)
            continue

        # Sigue en el mismo vértice, no hay viaje
        if curr_vertex == prev_vertex:
            mp.put(last_vertex_by_tag, tag, curr_vertex)
            continue

        # Hay un viaje A -> B
        A = prev_vertex
        B = curr_vertex

        vA = mp.get(vertices_info, A)
        vB = mp.get(vertices_info, B)

        dist_km = haversine(vA["lat"], vA["lon"], vB["lat"], vB["lon"])
        agua_B  = vB["avg_agua"]

        key   = (A, B)
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
            stats["count"]    += 1

        mp.put(edge_stats, key, stats)
        mp.put(last_vertex_by_tag, tag, curr_vertex)

    # Crear arcos a partir de los promedios acumulados
    edge_keys = mp.key_set(edge_stats)
    num_edges = lt.size(edge_keys)

    for i in range(num_edges):
        key   = lt.get_element(edge_keys, i)
        stats = mp.get(edge_stats, key)
        A, B  = key

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

def tags_first_last_3(tags_list):
    """
    Retorna un string con los tres primeros y tres últimos tags:
    [tag1, tag2, tag3, ..., tagN-2, tagN-1, tagN]
    Si hay 6 o menos tags, se muestran todos.
    """
    size = lt.size(tags_list)
    tags_py = []

    if size <= 6:
        for i in range(size):
            tags_py.append(str(lt.get_element(tags_list, i)))
    else:
        # primeros 3
        for i in range(3):
            tags_py.append(str(lt.get_element(tags_list, i)))
        tags_py.append("...")
        # últimos 3
        for i in range(size - 3, size):
            tags_py.append(str(lt.get_element(tags_list, i)))

    return "[" + ", ".join(tags_py) + "]"

# ----------------------------------------------------
# Funciones de consulta sobre el catálogo
# ----------------------------------------------------

def req_1(catalog, lat_origen, lon_origen, lat_destino, lon_destino, tag_id):
    """
    REQ. 1: Detectar el camino de un individuo entre dos puntos migratorios.

    - Usa Haversine para encontrar el punto de origen y destino (vértices más cercanos).
    - Usa los eventos YA ORDENADOS o los ordena por tiempo para esa grulla.
    - Construye un grafo individual con los movimientos de ese individuo.
    - Aplica DFS (módulo DFS) desde el punto que visita primero en el tiempo.
    - Reconstruye la ruta siguiendo la línea de tiempo entre esos dos puntos.
    - Calcula distancia total, número de puntos
      y arma tablas de ruta completa y 5 primeros + 5 últimos vértices.
    """
    start = get_time()

    vertices_info   = catalog["vertices_info"]
    events_by_tag   = catalog["events_by_tag"]
    event_to_vertex = catalog["event_to_vertex"]

    # 1. Verificar que exista el individuo
    eventos_tag = mp.get(events_by_tag, tag_id)
    if eventos_tag is None or lt.size(eventos_tag) == 0:
        return {
            "ok": False,
            "mensaje": f"El individuo {tag_id} no se encuentra en los datos.",
            "origen": None,
            "destino": None,
            "ruta_5_5": [],
            "ruta_completa": []
        }

    # 2. Encontrar puntos migratorios de origen y destino más cercanos (Haversine)
    origen_id, dist_o = find_closest_vertex(catalog, lat_origen, lon_origen)
    destino_id, dist_d = find_closest_vertex(catalog, lat_destino, lon_destino)

    if origen_id is None or destino_id is None:
        return {
            "ok": False,
            "mensaje": "No se encontraron puntos migratorios en el catálogo.",
            "origen": origen_id,
            "destino": destino_id,
            "ruta_5_5": [],
            "ruta_completa": []
        }

    # 3. Comprobar que el individuo pasa por esos puntos
    v_origen = mp.get(vertices_info, origen_id)
    v_dest   = mp.get(vertices_info, destino_id)

    if (v_origen is None or v_dest is None or
        not list_contains_str(v_origen["tags"], tag_id) or
        not list_contains_str(v_dest["tags"], tag_id)):
        return {
            "ok": False,
            "mensaje": f"El individuo {tag_id} no transita por el punto de origen y/o destino.",
            "origen": origen_id,
            "destino": destino_id,
            "ruta_5_5": [],
            "ruta_completa": []
        }

    # 4. Construir grafo dirigido SOLO con movimientos de este individuo
    num_eventos = lt.size(eventos_tag)
    g_ind = G.new_graph(num_eventos if num_eventos > 0 else 10)

    # Secuencia temporal de vértices (TDA lista) y posiciones donde aparece origen/destino
    vertex_sequence = lt.new_list()
    pos_origen = None
    pos_dest   = None

    prev_v = None
    for i in range(num_eventos):
        e = lt.get_element(eventos_tag, i)
        ev_id = e["event_id"]
        v_id  = mp.get(event_to_vertex, ev_id)

        if v_id is None:
            continue

        # Insertar vértice en grafo individual (no duplica si ya existe)
        G.insert_vertex(g_ind, v_id, None)

        # Añadir a la secuencia temporal
        lt.add_last(vertex_sequence, v_id)
        idx_seq = lt.size(vertex_sequence) - 1  # índice recién añadido

        # Guardar primera posición donde aparece origen y destino
        if v_id == origen_id and pos_origen is None:
            pos_origen = idx_seq
        if v_id == destino_id and pos_dest is None:
            pos_dest = idx_seq

        # Crear arco desde el vértice anterior si cambia de punto
        if prev_v is not None and v_id != prev_v:
            vA = mp.get(vertices_info, prev_v)
            vB = mp.get(vertices_info, v_id)
            if vA is not None and vB is not None:
                d = haversine(vA["lat"], vA["lon"], vB["lat"], vB["lon"])
            else:
                d = 0.0
            G.add_edge(g_ind, prev_v, v_id, d)

        prev_v = v_id

    # Si el individuo nunca pasa por uno de los puntos (por seguridad extra)
    if pos_origen is None or pos_dest is None:
        return {
            "ok": False,
            "mensaje": (f"El individuo {tag_id} no tiene suficientes registros "
                        f"para conectar el origen {origen_id} y el destino {destino_id}."),
            "origen": origen_id,
            "destino": destino_id,
            "ruta_5_5": [],
            "ruta_completa": []
        }

    # 5. Determinar el orden temporal: siempre vamos del que ocurre primero al que ocurre después
    if pos_origen <= pos_dest:
        start_idx, end_idx = pos_origen, pos_dest
        start_vertex = origen_id
        end_vertex   = destino_id
    else:
        start_idx, end_idx = pos_dest, pos_origen
        start_vertex = destino_id
        end_vertex   = origen_id

    # 6. Verificar que ambos vértices estén en el grafo individual
    if (not mp.contains(g_ind["vertices"], start_vertex) or
        not mp.contains(g_ind["vertices"], end_vertex)):
        return {
            "ok": False,
            "mensaje": (f"El individuo {tag_id} no tiene registros en el origen y/o "
                        f"destino seleccionados dentro de su trayectoria."),
            "origen": origen_id,
            "destino": destino_id,
            "ruta_5_5": [],
            "ruta_completa": []
        }

    # 7. Ejecutar DFS (módulo DFS) desde el vértice temporalmente más temprano
    dfs_result = DFS.dfs(g_ind, start_vertex)

    if not DFS.has_path_to(end_vertex, dfs_result):
        return {
            "ok": False,
            "mensaje": (f"No se encontró un camino DFS viable entre {start_vertex} y "
                        f"{end_vertex} para el individuo {tag_id}."),
            "origen": origen_id,
            "destino": destino_id,
            "ruta_5_5": [],
            "ruta_completa": []
        }

    # 8. Compactar el camino siguiendo la secuencia temporal entre start_idx y end_idx
    camino_vertices = lt.new_list()
    last_v = None
    for i in range(start_idx, end_idx + 1):
        v_id = lt.get_element(vertex_sequence, i)
        if last_v is None or v_id != last_v:
            lt.add_last(camino_vertices, v_id)
            last_v = v_id

    total_puntos = lt.size(camino_vertices)

    # 9. Calcular distancia total y armar información de cada vértice
    ruta_completa = []
    distancia_total = 0.0

    for idx in range(total_puntos):
        v_id = lt.get_element(camino_vertices, idx)
        v = mp.get(vertices_info, v_id)

        if v is None:
            lat = "Unknown"
            lon = "Unknown"
            num_grullas = "Unknown"
            tags_str = "Unknown"
        else:
            lat = v.get("lat", "Unknown")
            lon = v.get("lon", "Unknown")
            tags_list = v.get("tags", None)

            if tags_list is None:
                num_grullas = "Unknown"
                tags_str = "Unknown"
            else:
                num_grullas = lt.size(tags_list)
                tags_str = tags_first_last_3(tags_list)

        # Distancia al siguiente punto
        if idx < total_puntos - 1:
            next_v_id = lt.get_element(camino_vertices, idx + 1)
            v_next = mp.get(vertices_info, next_v_id)

            if v is not None and v_next is not None:
                d_next = haversine(
                    v["lat"], v["lon"],
                    v_next["lat"], v_next["lon"]
                )
            else:
                d_next = 0.0
        else:
            d_next = 0.0

        distancia_total += d_next

        fila = {
            "ID punto": v_id,
            "Posición (lat, lon)": f"({lat}, {lon})",
            "Num. grullas": num_grullas,
            "Tags (3 primeros y 3 últimos)": tags_str,
            "Distancia al siguiente (km)": round(d_next, 4)
        }
        ruta_completa.append(fila)

    # 10. Construir lista de 5 primeros y 5 últimos vértices (para la vista)
    if total_puntos <= 10:
        ruta_5_5 = ruta_completa[:]  # todos
    else:
        primeros_5 = ruta_completa[0:5]
        ultimos_5  = ruta_completa[-5:]
        ruta_5_5 = primeros_5 + ultimos_5

    primer_punto_camino = lt.get_element(camino_vertices, 0) if total_puntos > 0 else start_vertex

    end = get_time()
    delta = delta_time(start, end)
    
    resultado = {
        "ok": True,
        "mensaje": (f"El primer nodo del camino para el individuo {tag_id} "
                    f"(siguiendo su cronología) es el punto migratorio {primer_punto_camino}."),
        "origen": origen_id,      # vértice más cercano a lat/lon de entrada
        "destino": destino_id,    # vértice más cercano a lat/lon de salida
        "individuo": tag_id,
        "distancia_total_km": round(distancia_total, 4),
        "total_puntos": total_puntos,
        "ruta_5_5": ruta_5_5,
        "ruta_completa": ruta_completa,
        "tiempo_ms": delta
    }

    return resultado

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
