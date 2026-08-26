Repo di sviluppo delle componenti cpp della TableAPI di WindFlow.

In operazioni/ sono presenti varie applicazioni semplici in WF il cui scopo è di emulare la semantica relazionale tramite operatori nativi.
Sono state eseguite e testate, il resto dell'API cercherà di rendere generali gli schemi d'uso individuati.

In builder_pof/ sono presenti degli header cpp che incpsulano dei funtori che implementano le operazioni relazionali così da diminuire la quantità di codice da generare.
I builder tendono ad usare una sintassi simile a quella dei builder nativi con un paio di eccezioni.

Tutti i builder usano dei template in cui vanno inseriti gli struct di input, output o di keyBy se necessari.

Molti builder richiedono in input una lambda che implementi effettivamente la logica dell'operatore, ognuno richiede una firma specifica compatibile col funtore scritto secondo le firme richieste dagli operatori nativi.

Alcuni builder hanno due metodi separati di costruzione, build e build_keyed.
Le operazioni corrispondenti possono necessitare di instradamento per chiave per rispettare la propria semantica (ex. distinct) oppure non sono implementabili con parallelismo maggiore di uno senza di esso (ex. reduce).
Per via di come funziona l'inferenza di auto non si poteva avere un metodo che rendesse un operatore a volte con chiave e a volte no (la chiave con par=1 è overhead inutile) dunque sono stati separati i metodi.

Per la compilazione è richiesta WindFlow e FastFlow nella root, il cmakelist fornito poi fa il resto.
