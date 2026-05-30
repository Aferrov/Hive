import sys

def main():
    palabra_actual = None
    contador_actual = 0

    for linea in sys.stdin:
        linea = linea.strip()
        if not linea:
            continue
            
        palabra, valor = linea.split('\t', 1)

        try:
            valor = int(valor)
        except ValueError:
            continue

        if palabra_actual == palabra:
            contador_actual += valor
        else:
            if palabra_actual:
                print(f"{palabra_actual}\t{contador_actual}")
            
            palabra_actual = palabra
            contador_actual = valor

    if palabra_actual == palabra:
        print(f"{palabra_actual}\t{contador_actual}")

if __name__ == "__main__":
    main()
