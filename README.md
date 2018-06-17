# Autors
Antonio Ciordia.

Nil Bardou. 

# PASSES A SEGUIR PEL FUNCIONAMENT DEL PROGRAMA SEQÜENCIAL
- pas 1: Activar les claus d'Amazon.
- pas 2: Agafar les claus d'Amazon i ficar-les dintre dels arxius: mapper.py, reducer.py, 
principal.py.
- pas 3: Creem un BUCKET al nostre servidor de AWS (Amazon Web Service), al qual pujem els 
arxius.
- pas 4: Configurem les funciones lambda. En el nostre cas tenim 2 funcions lambda, una d'aquestes es troba al 
Mapper que s'encarrega de contar quants cops apareix cada paraula i la segona lambda es al Reducer, la qual s'encarrega de juntar els resultats de cadascun dels mappers.
- pas 5: Ens coloquem al directori de la pràctica i executem el nostre fitxer Python 
(Comanda: python principal.py)
- pas 6: Observem els resultats obtinguts. 
