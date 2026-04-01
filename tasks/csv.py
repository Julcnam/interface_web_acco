import pandas as pd
import numpy as np
import os
from tasks.db import connect


# Créer le cvs depuis la base de données sql
def get_csv():

    conn=connect()

    print("Création du csv ... ")

    accords_infos = conn.sql("SELECT * FROM  metadonnee").df()


    # Gestion des doublons 

    accords_infos = accords_infos.drop_duplicates(subset="reference", keep="first")

    # Gestion des NAs

    accords_infos["signataires"] = accords_infos["signataires"].replace("", np.nan)
    accords_infos["thematique"] = accords_infos["thematique"].replace("", np.nan)



    # Dates 

    accords_infos["date_signature"] = pd.to_datetime(
        accords_infos["date_signature"], format="%d/%m/%Y", errors="coerce"
    )

    accords_infos["date_publication"] = pd.to_datetime(
        accords_infos["date_publication"], format="%d/%m/%Y", errors="coerce"
    )

    accords_infos["date_application_debut"] = pd.to_datetime(
        accords_infos["date_application_debut"], format="%d/%m/%Y", errors="coerce"
    )

    accords_infos["date_application_fin"] = pd.to_datetime(
        accords_infos["date_application_fin"], format="%d/%m/%Y", errors="coerce"
    )


    # Natures 

    accords_infos["ACCORD"] = accords_infos["nature"].str.contains("ACCORD", case=False, na=False).map({True: "Oui", False: "Non"})
    accords_infos["AVENANT"] = accords_infos["nature"].str.contains("AVENANT", case=False, na=False).map({True: "Oui", False: "Non"})


    # Signataires 

    def detect_union(pattern):
        return accords_infos["signataires"].str.contains(
            pattern, case=False, na=False
        ).map({True: "oui", False: "non"})

    accords_infos["CGT"] = detect_union("CGT")
    accords_infos["CFDT"] = detect_union("CFDT")
    accords_infos["CFE_CGC"] = detect_union("CFE-CGC")
    accords_infos["CFTC"] = detect_union("CFTC")
    accords_infos["CGT_FO"] = detect_union("CGT-FO")
    accords_infos["SOLIDAIRES"] = detect_union("SOLIDAIRES")
    accords_infos["UNSA"] = detect_union("UNSA")
    accords_infos["SyndicAutre"] = detect_union("Autre")

    accords_infos["PasSyndic"] = accords_infos["signataires"].isna().map(
        {True: "oui", False: "non"}
    )

    # APE 

    accords_infos["ape_2"] = (
        accords_infos["ape"].astype(str).str[:-3]
    )

    accords_infos["ape_2"] = pd.to_numeric(
        accords_infos["ape_2"], errors="coerce"
    )

    def recode_secteur(x):
        if pd.isna(x):
            return np.nan
        if 1 <= x <= 3: return "A_agri"
        if 5 <= x <= 9: return "B_indusextract"
        if 10 <= x <= 33: return "C_indusmanufac"
        if x == 35: return "D_electgaz"
        if 36 <= x <= 39: return "E_eaudechet"
        if 41 <= x <= 43: return "F_construct"
        if 45 <= x <= 47: return "G_commerce"
        if 49 <= x <= 53: return "H_transport"
        if 55 <= x <= 56: return "I_hebergresto"
        if 58 <= x <= 63: return "J_infocom"
        if 64 <= x <= 66: return "K_financassur"
        if x == 68: return "L_immob"
        if 69 <= x <= 75: return "M_scienctechniq"
        if 77 <= x <= 82: return "N_adminsoutien"
        if x == 84: return "O_adminpublic"
        if x == 85: return "P_enseignmnt"
        if 86 <= x <= 88: return "Q_santsocial"
        if 90 <= x <= 93: return "R_artsrecreatif"
        if 94 <= x <= 96: return "S_autreservic"
        if 97 <= x <= 98: return "T_menage"
        if x == 99: return "U_extraterrito"
        return np.nan

    accords_infos["secteur"] = accords_infos["ape_2"].apply(recode_secteur)


    # Secteur recodé 

    def recode_secteur_r(x):
        if x == "A_agri": return "Agriculture"
        if x in ["B_indusextract", "C_indusmanufac"]: return "Industries"
        if x in ["E_eaudechet", "D_electgaz"]: return "Energies"
        if x == "F_construct": return "Construction"
        if x in ["G_commerce", "I_hebergresto"]: return "Commerce"
        if x in ["L_immob", "S_autreservic", "T_menage", "R_artsrecreatif", "N_adminsoutien"]:
            return "Services"
        if x == "H_transport": return "Transports"
        if x == "K_financassur": return "Finance"
        if x == "M_scienctechniq": return "Services techniques"
        if x == "J_infocom": return "Information"
        if x in ["Q_santsocial", "O_adminpublic", "P_enseignmnt"]:
            return "Services sociaux"
        return np.nan

    accords_infos["secteur_r"] = accords_infos["secteur"].apply(recode_secteur_r)


    # Themes

    themes_dict = {
        "t_ACC_METH_PENIB": r"Accords de méthode \(pénibilité\)",
        "t_ACC_METH_PSE": r"Accords de méthode \(PSE\)",
        "t_AMEN_TPS_TRAV": r"Aménagement du temps de travail",
        "t_AUTRE": r"Autre, précisez",
        "t_COND_TRAV": r"conditions de travail",
        "t_AUTRE_DUREE": r"Autres dispositions durée",
        "t_AUTRE_EGALITE": r"Autres dispositions Egalité",
        "t_AUTRE_EMPLOI": r"Autres dispositions emploi",
        "t_CALEND_NEGO": r"Calendrier des négociations",
        "t_CLASSIF": r"Classifications",
        "t_COMM_PARIT": r"Commissions paritaires",
        "t_CET": r"Compte épargne temps",
        "t_SANTE": r"Couverture complémentaire santé",
        "t_DON_JOUR": r"Dispositifs don de jour",
        "t_ACTIONS": r"Distribution d'actions gratuites",
        "t_DECONNEXION": r"Droit à la déconnexion",
        "t_DROIT_SYND": r"Droit syndical",
        "t_DUREE_TRAV": r"Durée collective",
        "t_EGAL_SAL": r"Egalité salariale",
        "t_ELECT_PRO": r"Elections professionnelles",
        "t_EVOL_PRIME": r"Evolution des primes",
        "t_EVOL_SAL": r"Evolution des salaires",
        "t_FIN_CONFLIT": r"Fin de conflit",
        "t_CONGES": r"Fixation des congés",
        "t_FORFAITS": r"Forfaits",
        "t_FORMATION": r"Formation professionnelle",
        "t_GPEC": r"GPEC",
        "t_H_SUPP": r"Heures supplémentaires",
        "t_INDEM": r"Indemnités",
        "t_INTERESSEMENT": r"Intéressement",
        "t_MESURES_AGE": r"Mesures d'âge",
        "t_MOBILITE": r"Mobilité",
        "t_NON_DISCRIM": r"Non discrimination",
        "t_PARTICIPATION": r"Participation",
        "t_PEE_PEG": r"PEE ou PEG",
        "t_PEI": r"PEI",
        "t_PENIBILITE": r"Pénibilité du travail",
        "t_PERCO": r"PERCO",
        "t_PERF_COLL": r"Performance collective",
        "t_PREVOYANCE": r"Prévoyance collective",
        "t_PRIME_PROFIT": r"Prime de partage des profits",
        "t_QVT": r"QVT",
        "t_REPRISE_DONNEES": r"Reprise des données",
        "t_RETRAITE": r"Retraite complémentaire",
        "t_RUP_CONV_COLL": r"Rupture conventionnelle collective",
        "t_RPS": r"risques psycho",
        "t_SUPPL_INTER": r"Supplément d'intéressement",
        "t_SUPPL_PART": r"Supplément de participation",
        "t_SYST_PRIME": r"Système de prime",
        "t_SYST_REMUN": r"Système de rémunération",
        "t_TELETRAVAIL": r"Télétravail",
        "t_TPS_PARTIEL": r"Travail à temps partiel",
        "t_TRAV_NUIT": r"Travail de nuit",
        "t_TRAV_DIM": r"Travail du dimanche",
        "t_HAND": r"Travailleurs handicapés"
    }

    for abrev, pattern in themes_dict.items():
        accords_infos[abrev] = (
            accords_infos["thematique"]
            .str.contains(pattern, case=False, na=False)
            .map({True: "Oui", False: "Non"})
        )


    # SIREN

    accords_infos["siren"] = accords_infos["siret"].astype(str).str[:9]

    accords_infos["plan_action"] = accords_infos["plan_action"].map({True: "Oui", False: "Non"})
    accords_infos["charte"] = accords_infos["charte"].map({True: "Oui", False: "Non"})
    accords_infos["nao"] = accords_infos["nao"].map({True: "Oui", False: "Non"})
    accords_infos["proces_verbal"] = accords_infos["proces_verbal"].map({True: "Oui", False: "Non"})    
    # accords_infos["groupe"] = accords_infos["siren"].map(lambda x: "Groupe" if str(x).startswith("356") else "Indépendant")
    accords_infos["groupe"] = accords_infos["groupe"].map({True: "Oui", False: "Non"})
    accords_infos["ues"] = accords_infos["ues"].map({True: "Oui", False: "Non"})


    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bdd_dir = os.path.join(base_dir, "csv")
    os.makedirs(bdd_dir, exist_ok=True)  # crée le dossier si absent
    # Chemin complet vers le csv
    db_path = os.path.join(bdd_dir, "métadonnées.csv")


    accords_infos.to_csv(db_path,index=False)

    print("Csv créé avec succcès.")
