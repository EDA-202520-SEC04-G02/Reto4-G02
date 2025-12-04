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
    # TODO: Imprimir el resultado del requerimiento 2
    pass


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
    pass


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    pass

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
