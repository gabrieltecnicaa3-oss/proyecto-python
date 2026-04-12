import os

carpeta = r'c:\Users\usuar\OneDrive\Desktop\python\remitos'
if not os.path.exists(carpeta):
    os.makedirs(carpeta)
    print(f"✅ Carpeta '{carpeta}' creada exitosamente")
else:
    print(f"✅ Carpeta '{carpeta}' ya existe")
