
1) Tous les messages doivent être stockés afin de pouvoir créer des statistiques, et chaque drone crée 100 Go de données. PrestaCop doit donc posséder un système d’information fiable. Toutes les informations seraient alors stockées dans une base de données (type S3). On crée les statistiques grâce à un job streaming.

2) Il se peut que le drone n’arrive pas à lire certaines plaques. Il y aurait donc besoin d’une intervention humaine afin de palier à ce problème. Le système pourrait posséder une colonne qui indique si une infraction a été traitée ou à besoin d’une intervention humaine.

3) L’erreur de PrestaCop est que leur programme utilise trop de mémoires pour pouvoir envoyer des données aux ordinateurs de la police de New-York, qui possède des vieux ordinateurs qui ne sont pas puissants. Le programme est trop volumineux car tout est centralisée sur le réseau de la NYPD.

4) PrestaCop n’indique pas l’état de la machine (niveau de batterie restant par exemple), ni si le drone a besoin d'assistance.