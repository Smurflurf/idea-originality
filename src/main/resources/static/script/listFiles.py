import os

verzeichnis = "."
ausgabe_datei = "javaskript.txt"

with open(ausgabe_datei, "w", encoding="utf-8") as ausgabe:
    for dateiname in os.listdir(verzeichnis):
        if dateiname.endswith(".js"):
            pfad = os.path.join(verzeichnis, dateiname)
            try:
                with open(pfad, "r", encoding="utf-8") as f:
                    inhalt = f.read()
                    ausgabe.write(f"\n{dateiname}: \n<[\n{inhalt}\n]>\n")
            except Exception as e:
                ausgabe.write(f"Fehler beim Lesen von {dateiname}: {e}\n")
