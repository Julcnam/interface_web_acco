import duckdb
import os
from pathlib import Path
import pandas as pd
import lxml.etree as ET
from alive_progress import alive_bar
import re
from config import DOWNLOAD_PATH
from unidecode import unidecode


# Connexion à DuckDB
def connect():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bdd_dir = os.path.join(base_dir, "bdd")
    os.makedirs(bdd_dir, exist_ok=True)

    db_path = os.path.join(bdd_dir, "metadonnee.duckdb")
    return duckdb.connect(db_path)


# Fonctions d'extraction et de transformation des données XML
def get_text(root, tag):
    el = root.find(f".//{tag}")
    return el.text.strip() if el is not None and el.text else None

# Concaténer les valeurs d'un même champ séparées par un pipe (|) pour les champs à valeurs multiples
def concat_labels(root, xpath, sep=" | "):
    values = [
        el.text.strip()
        for el in root.findall(xpath)
        if el.text and el.text.strip()
    ]
    return sep.join(values) if values else None

# Fonction  pour détecter la présence de mots-clés dans le titre, en ignorant les variations de casse et les espaces 
def contains(text, pattern):
    text = unidecode(text)
    pattern = unidecode(pattern)
                   
    if not text:
        return False
    return re.search(pattern, text, re.IGNORECASE) is not None


# Parser XML 
def parse_xml(xml_path: Path) -> dict:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    titre = get_text(root, "TITRE_TXT") or ""

    return {
        "reference": get_text(root, "NUMERO"),
        "titre": titre,
        "ape": get_text(root, "CODE_APE"),
        "idcc": get_text(root, "CODE_IDCC"),
        "origine": get_text(root, "ORIGINE"),
        "nature": get_text(root, "NATURE"),
        "raison_sociale": get_text(root, "RAISON_SOCIALE"),
        "siret": get_text(root, "SIRET"),
        "secteur": get_text(root, "SECTEUR"),
        "signataires": concat_labels(root, ".//SYNDICAT/LIBELLE"),
        "thematique": concat_labels(root, ".//THEME/LIBELLE"),

        # BOOLÉENS ROBUSTES
        "plan_action": contains(titre, r"plan d[' ]action"),
        "charte": contains(titre, r"charte"),
        "nao": contains(titre, r"\bnao\b") or contains(titre, r"négociation annuelle obligatoire"),
        "proces_verbal": contains(titre, r"proc[èe]s verbal"),
        "groupe": contains(titre, r"groupe"),
        "ues": contains(titre, r"\bues\b") or contains(titre, r"unité économique et sociale"),

        # DATES
        "date_signature": get_text(root, "DATE_TEXTE"),
        "date_publication": get_text(root, "DATE_DIFFUSION"),
        "date_application_debut": get_text(root, "DATE_EFFET"),
        "date_application_fin": get_text(root, "DATE_FIN"),
    }



# Insertion des métadonnées dans DuckDB
def insert_metadata():
    conn = connect()
    base_path = Path(DOWNLOAD_PATH)

    xml_files = list(base_path.rglob("*.xml"))
    if not xml_files:
        print("Aucun fichier XML trouvé.")
        return

    rows = []

    print(f"Parsing de {len(xml_files)} fichiers XML...")

    with alive_bar(len(xml_files), title="Parsing XML") as bar:
        for path in xml_files:
            try:
                rows.append(parse_xml(path))
            except Exception as e:
                print(f"Erreur {path.name}: {e}")
            bar()

    df = pd.DataFrame(rows)

    print("Insertion DuckDB...")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadonnee AS
        SELECT * FROM df LIMIT 0
    """)

    conn.register("df", df)

    conn.execute("""INSERT INTO metadonnee SELECT * FROM df WHERE reference IS NOT NULL AND NOT EXISTS (SELECT 1 FROM metadonnee m WHERE m.reference = df.reference)""")

    conn.unregister("df")

    print("Insertion terminée.")