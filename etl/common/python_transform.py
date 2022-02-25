
def concatGenusSpecies(document):
    if "genus" in document and "species" in document and "genusSpecies" not in document:
        document["genusSpecies"] = document["genus"] + " " + document["species"]

mapping_dict = {
    "study_name": "studyName"
}

function_dict = {
    "genusSpecies" : concatGenusSpecies
}





def do_transform(document, uri_data_index):

#1 on uniformise en 1.3/2.X On transforme vers un modèle commun
# c'est un mapping tout les champs hétérogènes sont uniformisés: name ==> studyName, accNumb ==> ACCESSION_NUMBER, etc....

#2 on enrichis en ajoutant genusSpecies ou en allant chercher les obsVarDbId etc...

    for (oldkey, newkey) in mapping_dict.items():
        if oldkey in document:
            document[newkey] = document.pop(oldkey)

    for (newkey, transform_function) in function_dict.items():
         transform_function(document)

    return document
