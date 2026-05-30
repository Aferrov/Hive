import sys

def main():
    palabra_actual = None
    archivos_asociados = set()

    for linea in sys.stdin:
        linea = linea.strip()
        if not linea:
            continue
            
        palabra, nombre_archivo = linea.split('\t', 1)

        if palabra_actual == palabra:
            archivos_asociados.add(nombre_archivo)
        else:
            if palabra_actual:
                print(f"{palabra_actual}: {list(archivos_asociados)}")
            
            palabra_actual = palabra
            archivos_asociados = {nombre_archivo}

    if palabra_actual == palabra:
        print(f"{palabra_actual}: {list(archivos_asociados)}")

if __name__ == "__main__":
    main()