# -------------------------------------------
import sys
import App.logic as logic
from tabulate import tabulate 
import os
data_dir = os.path.dirname(os.path.realpath('__file__')) + '/Data/'
from DataStructures.List import array_list as lt
from DataStructures.Map import map_linear_probing as mp
# -------------------------------------------


def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO DONE: Llamar la función de la lógica donde se crean las estructuras de datos
    control = logic.new_logic()
    return control


def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    #TODO DONE: Realizar la carga de datos
    # file_name = "1000_cranes_mongolia_small.csv"
    # file_name = "1000_cranes_mongolia_30pct.csv"
    # file_name = "1000_cranes_mongolia_80pct.csv"
    file_name = "1000_cranes_mongolia_large.csv"

    file_path = data_dir + file_name

    elapsed = logic.load_data(control, file_path)

    graph_dist = control["graph_dist"]
    graph_agua = control["graph_agua"]
    events_by_tag = control["events_by_tag"]

    # Total de grullas reconocidas = número de llaves del mapa events_by_tag
    total_grullas = mp.size(events_by_tag)

    # Total de eventos cargados = suma de tamaños de las listas por tag
    total_eventos = 0
    tag_keys = mp.key_set(events_by_tag)
    for i in range(lt.size(tag_keys)):
        tag = lt.get_element(tag_keys, i)
        ev_list = mp.get(events_by_tag, tag)
        total_eventos += lt.size(ev_list)

    # Número de nodos (vértices) = tamaño del mapa de vértices del grafo
    num_vertices = mp.size(graph_dist["vertices"])

    # Número de arcos en cada grafo
    num_edges_dist = graph_dist["num_edges"]
    num_edges_agua = graph_agua["num_edges"]

    print("\n=======================================")
    print("           CARGA DE DATOS              ")
    print("=======================================")
    print(f"Total de grullas reconocidas: {total_grullas}")
    print(f"Total de eventos cargados:    {total_eventos}")
    print(f"Total de nodos del grafo:     {num_vertices}")
    print(f"Total de arcos (distancia):   {num_edges_dist}")
    print(f"Total de arcos (agua):        {num_edges_agua}")
    print(f"Tiempo total de carga:        {elapsed:.3f} ms")

    # Mostrar primeros y últimos vértices
    first, last = logic.get_vertices_samples(control, n=5)

    print("\n=======================================")
    print("      DETALLE DE NODOS (VÉRTICES)      ")
    print("=======================================\n")

    if first:
        print("--- Primeros 5 Nodos ---\n")
        print(tabulate(first, headers="keys", tablefmt="grid"))
        print()

    if last:
        print("--- Últimos 5 Nodos ---\n")
        print(tabulate(last, headers="keys", tablefmt="grid"))
        print()

    return elapsed

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    print("\n=== REQ. 1: Camino de un individuo ===")
    
    tag_id = input("Identificador del individuo (tag-local-identifier): ").strip()

    lat_o = float(input("Latitud del punto de origen: "))
    lon_o = float(input("Longitud del punto de origen: "))
    lat_d = float(input("Latitud del punto de destino: "))
    lon_d = float(input("Longitud del punto de destino: "))

    result = logic.req_1(control, lat_o, lon_o, lat_d, lon_d, tag_id)
    print(f"Tiempo de ejecución del requerimiento 1: {result['tiempo_ms']:.3f} ms")
    print("\n" + result["mensaje"])

    if not result["ok"]:
        print(f"Origen aproximado: {result['origen']}")
        print(f"Destino aproximado: {result['destino']}")
        return

    print(f"\nPunto migratorio de origen (más cercano): {result['origen']}")
    print(f"Punto migratorio de destino (más cercano): {result['destino']}")
    print(f"Individuo: {result['individuo']}")
    print(f"Distancia total del camino: {result['distancia_total_km']:.4f} km")
    print(f"Total de puntos en la ruta: {result['total_puntos']}")

    # Usamos la ruta completa que devuelve la lógica
    ruta = result["ruta_completa"]
    if not ruta:
        print("\nNo se pudo construir la ruta.")
        return

    n = 5
    total = len(ruta)

    if total <= n:
        # Pocos puntos: mostramos todo como “primeros” y todo como “últimos”
        primeros = ruta
        ultimos = ruta
    else:
        primeros = ruta[0:n]
        ultimos = ruta[-n:]

    print("\n--- Primeros 5 vértices de la ruta ---\n")
    print(tabulate(primeros, headers="keys", tablefmt="grid"))

    print("\n--- Últimos 5 vértices de la ruta ---\n")
    print(tabulate(ultimos, headers="keys", tablefmt="grid"))

def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    print("\n=== REQ. 2: Movimientos de un nicho biológico alrededor de un área ===")

    lat_o = float(input("Latitud del punto de origen: "))
    lon_o = float(input("Longitud del punto de origen: "))
    lat_d = float(input("Latitud del punto de destino: "))
    lon_d = float(input("Longitud del punto de destino: "))
    radio = float(input("Radio del área de interés (km): "))

    result = logic.req_2(control, lat_o, lon_o, lat_d, lon_d, radio)

    # Tiempo de ejecución (si viene en el resultado)
    if "tiempo_ms" in result:
        print(f"\nTiempo de ejecución del requerimiento 2: {result['tiempo_ms']:.3f} ms")

    # Si falla, mensaje simple y salimos
    if not result["success"]:
        print("\nNo se pudo encontrar un camino viable para el requerimiento 2.")
        print(f"Vértice de origen aproximado: {result.get('origin_vertex', 'Unknown')}")
        print(f"Vértice de destino aproximado: {result.get('dest_vertex', 'Unknown')}")
        if "error" in result:
            print("Detalle:", result["error"])
        return

    # Éxito: resumen básico
    print(f"\nVértice de origen (más cercano): {result['origin_vertex']}")
    print(f"Vértice de destino (más cercano): {result['dest_vertex']}")
    print(f"Distancia total de desplazamiento: {result['total_distance_km']:.4f} km")
    print(f"Total de puntos en el camino: {result['total_points']}")

    # Mensaje sobre el último nodo dentro del área de interés
    last_inside = result["last_inside_vertex"]
    radio_km = result["radio_km"]

    if last_inside is None:
        msg_area = (
            f"Ningún nodo de la ruta se encuentra dentro del área de interés "
            f"de radio {radio_km} km alrededor del origen."
        )
    else:
        msg_area = (
            f"El último nodo dentro del área de interés (radio {radio_km} km) "
            f"es el vértice con id: {last_inside}."
        )

    print("\n" + msg_area)

    # Función auxiliar para convertir lista TDA a lista Python
    def tda_to_py_list(tda_list):
        rows = []
        n = lt.size(tda_list)
        for i in range(n):
            rows.append(lt.get_element(tda_list, i))
        return rows

    primeros = tda_to_py_list(result["first_vertices"])
    ultimos  = tda_to_py_list(result["last_vertices"])

    print("\n--- Cinco primeros vértices de la ruta ---\n")
    if primeros:
        print(tabulate(primeros, headers="keys", tablefmt="grid"))
    else:
        print("No hay vértices para mostrar.")

    print("\n--- Cinco últimos vértices de la ruta ---\n")
    if ultimos:
        print(tabulate(ultimos, headers="keys", tablefmt="grid"))
    else:
        print("No hay vértices para mostrar.")

def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    pass


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    print("\n=== REQ. 4: Corredor hídrico óptimo (MST) ===")
    lat_o = float(input("Latitud del punto de origen: "))
    lon_o = float(input("Longitud del punto de origen: "))

    result = logic.req_4(control, lat_o, lon_o)

    print("\n" + result["mensaje"])

    # Siempre imprimimos el tiempo, exista o no red viable
    if "tiempo_ms" in result:
        print(f"Tiempo de ejecución REQ.4: {result['tiempo_ms']:.3f} ms")

    if not result["ok"]:
        print(f"Punto migratorio de origen (más cercano): {result['origen']}")
        return

    print(f"\nPunto migratorio de origen (más cercano): {result['origen']}")
    print(f"Total de puntos en el corredor: {result['total_puntos']}")
    print(f"Total de individuos en el corredor: {result['total_individuos']}")
    print(f"Distancia total del corredor a fuentes hídricas: {result['distancia_total_agua']:.4f} km")

    ruta = result["ruta_completa"]
    if not ruta:
        print("\nNo se pudo construir el corredor migratorio.")
        return

    n = 5
    total = len(ruta)
    if total <= n:
        primeros = ruta
        ultimos = ruta
    else:
        primeros = ruta[0:n]
        ultimos = ruta[-n:]

    print("\n--- Primeros 5 vértices del corredor ---\n")
    print(tabulate(primeros, headers="keys", tablefmt="grid"))

    print("\n--- Últimos 5 vértices del corredor ---\n")
    print(tabulate(ultimos, headers="keys", tablefmt="grid"))


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    print("\n=== REQ. 5: Ruta migratoria más eficiente entre dos puntos ===")

    lat_o = float(input("Latitud del punto de origen: "))
    lon_o = float(input("Longitud del punto de origen: "))
    lat_d = float(input("Latitud del punto de destino: "))
    lon_d = float(input("Longitud del punto de destino: "))

    print("\nSeleccione el tipo de grafo a usar:")
    print("1. Grafo por distancia de desplazamiento")
    print("2. Grafo por distancia a fuentes hídricas")
    opcion = input("Opción (1/2): ").strip()

    if opcion == "2":
        tipo_grafo = "agua"
    else:
        tipo_grafo = "dist"

    result = logic.req_5(control, lat_o, lon_o, lat_d, lon_d, tipo_grafo)

    # Mensaje principal
    mensaje = result.get("mensaje", "Sin mensaje disponible.")
    print("\n" + mensaje)

    # Tiempo de ejecución
    if "tiempo_ms" in result:
        print(f"Tiempo de ejecución REQ.5: {result['tiempo_ms']:.3f} ms")

    # Si hubo error o no hay ruta
    if not result.get("ok", False):
        print(f"Punto migratorio de origen (más cercano): {result.get('origen', 'Unknown')}")
        print(f"Punto migratorio de destino (más cercano): {result.get('destino', 'Unknown')}")
        return

    print(f"\nPunto migratorio de origen (más cercano): {result.get('origen', 'Unknown')}")
    print(f"Punto migratorio de destino (más cercano): {result.get('destino', 'Unknown')}")
    print(f"Métrica usada: {result.get('metrica', 'Unknown')}")
    print(f"Costo total del camino: {result.get('costo_total', 0.0)}")
    print(f"Total de puntos en la ruta: {result.get('total_puntos', 0)}")
    print(f"Total de segmentos en la ruta: {result.get('total_segmentos', 0)}")

    # ----- Convertir ruta_completa (TDA) a lista de Python -----
    ruta_tda = result.get("ruta_completa", lt.new_list())
    total = lt.size(ruta_tda)

    if total == 0:
        print("\nNo hay detalles de ruta para mostrar.")
        return

    ruta_py = []
    i = 0
    while i < total:
        fila = lt.get_element(ruta_tda, i)
        ruta_py.append(fila)
        i += 1

    # 5 primeros y 5 últimos
    n = 5
    if n > total:
        n = total

    primeros = ruta_py[0:n]
    ultimos = ruta_py[-n:]

    print("\n--- Primeros 5 vértices de la ruta ---\n")
    print(tabulate(primeros, headers="keys", tablefmt="grid"))

    print("\n--- Últimos 5 vértices de la ruta ---\n")
    print(tabulate(ultimos, headers="keys", tablefmt="grid"))


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    
    print("\n=== REQ. 6: Subredes hídricas aisladas (Componentes Conexas) ===")

    result = logic.req_6(control)

    # Mensaje principal
    mensaje = result.get("mensaje", "Sin mensaje disponible.")
    print("\n" + mensaje)

    # Tiempo de ejecución
    if "tiempo_ms" in result:
        print(f"Tiempo de ejecución REQ.6: {result['tiempo_ms']:.3f} ms")

    # Caso error / no hay subredes
    if not result.get("ok", False):
        print(f"Total de subredes identificadas: {result.get('total_subredes', 0)}")
        return

    print(f"Total de subredes hídricas identificadas: {result.get('total_subredes', 0)}")

    subredes = result.get("subredes_top", lt.new_list())
    n_sub = lt.size(subredes)

    if n_sub == 0:
        print("\nNo se encontraron subredes hídricas viables.")
        return

    print("\n=== Las 5 subredes hídricas más grandes ===")

    i = 0
    while i < n_sub:
        sub = lt.get_element(subredes, i)

        print(f"\n--- Subred #{i+1} (ID = {sub.get('id_subred', 'Unknown')}) ---")
        print(f"Número de puntos migratorios: {sub.get('num_vertices', 'Unknown')}")
        print(f"Latitud mínima:  {sub.get('min_lat', 'Unknown')}")
        print(f"Latitud máxima:  {sub.get('max_lat', 'Unknown')}")
        print(f"Longitud mínima: {sub.get('min_lon', 'Unknown')}")
        print(f"Longitud máxima: {sub.get('max_lon', 'Unknown')}")
        print(f"Total de individuos en la subred: {sub.get('total_individuals', 'Unknown')}")
        print(f"Tags (3 primeros y 3 últimos): {sub.get('tags_sample', 'Unknown')}")

        # ---- Primeros 3 puntos migratorios ----
        print("\n> Primeros 3 puntos migratorios:")
        primeros_tda = sub.get("first_points", lt.new_list())
        primeros_py = []
        n_prim = lt.size(primeros_tda)
        j = 0
        while j < n_prim:
            primeros_py.append(lt.get_element(primeros_tda, j))
            j += 1

        if primeros_py:
            print(tabulate(primeros_py, headers="keys", tablefmt="grid"))
        else:
            print("No hay puntos para mostrar.")

        # ---- Últimos 3 puntos migratorios ----
        print("\n> Últimos 3 puntos migratorios:")
        ultimos_tda = sub.get("last_points", lt.new_list())
        ultimos_py = []
        n_ult = lt.size(ultimos_tda)
        j = 0
        while j < n_ult:
            ultimos_py.append(lt.get_element(ultimos_tda, j))
            j += 1

        if ultimos_py:
            print(tabulate(ultimos_py, headers="keys", tablefmt="grid"))
        else:
            print("No hay puntos para mostrar.")

        i += 1


# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 6:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
