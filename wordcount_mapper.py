import sys
import re

def limpiar_acentos(texto):
    reemplazos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u'}
    for con_acento, sin_acento in reemplazos.items():
        texto = texto.replace(con_acento, sin_acento)
    return texto

def main():
    for linea in sys.stdin:
        linea = linea.strip().lower()
        
        linea = limpiar_acentos(linea)
        
        palabras = re.findall(r'\b[a-zñ]+\b', linea)
        
        for palabra in palabras:
            print(f"{palabra}\t1")

if __name__ == "__main__":
    main()