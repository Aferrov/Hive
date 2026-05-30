import sys
import re
import os

def limpiar_acentos(texto):
    # Unificamos las vocales con tilde o diéresis
    reemplazos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u'}
    for con_acento, sin_acento in reemplazos.items():
        texto = texto.replace(con_acento, sin_acento)
    return texto

def main():
    # Obtener el nombre del archivo origen desde las variables de entorno de Hadoop
    ruta_archivo = os.environ.get('map_input_file', 'documento_desconocido')
    nombre_archivo = ruta_archivo.split('/')[-1]

    for linea in sys.stdin:
        # 1. Pasar a minúsculas y limpiar espacios extremos
        linea = linea.strip().lower()
        
        # 2. Reemplazar acentos de forma segura (ej: 'canción' -> 'cancion')
        linea = limpiar_acentos(linea)
        
        # 3. Extraer solo palabras ignorando números/signos y protegiendo la 'ñ'
        palabras = re.findall(r'\b[a-zñ]+\b', linea)
        
        for palabra in palabras:
            # Enviamos al reducer con formato: palabra \t nombre_archivo
            print(f"{palabra}\t{nombre_archivo}")

if __name__ == "__main__":
    main()