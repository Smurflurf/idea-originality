import os

def recursiveSearch(ausgabe, verzeichnis = ".", endung = "css"):
    try:
        for dateiname in os.listdir(verzeichnis):
            if dateiname.endswith(f".{endung}"):
                print(dateiname)
                pfad = os.path.join(verzeichnis, dateiname)
                try:
                    with open(pfad, "r", encoding="utf-8") as f:
                        inhalt = f.read()
                        ausgabe.write(f"\n{pfad} - {dateiname}: \n<[\n{inhalt}\n]>\n")
                except Exception as e:
                    ausgabe.write(f"Fehler beim Lesen von {dateiname}: {e}\n")
            elif os.path.isdir(os.path.join(verzeichnis, dateiname)):
                recursiveSearch(ausgabe, verzeichnis=os.path.join(verzeichnis, dateiname))
    except PermissionError:
        pass


if __name__ == "__main__":
    ausgabe_datei = "css.txt"
    
    with open(ausgabe_datei, "w", encoding="utf-8") as ausgabe:
        recursiveSearch(ausgabe=ausgabe)