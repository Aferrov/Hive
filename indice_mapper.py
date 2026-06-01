import sys
import re
import os

def limpiar_acentos(texto):
    reemplazos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u'}
    for con_acento, sin_acento in reemplazos.items():
        texto = texto.replace(con_acento, sin_acento)
    return texto

def main():
    ruta_archivo = os.environ.get('map_input_file', 'documento_desconocido')
    nombre_archivo = ruta_archivo.split('/')[-1]

    for linea in sys.stdin:
        linea = linea.strip().lower()
        
        linea = limpiar_acentos(linea)
        
        palabras = re.findall(r'\b[a-zñ]+\b', linea)
        
        for palabra in palabras:
            print(f"{palabra}\t{nombre_archivo}")

if __name__ == "__main__":
    main()