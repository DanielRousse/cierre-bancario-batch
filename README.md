# Cierre bancario con Spring Batch

**Autor:** Jonathan Daniel Reyes Gordillo

## Cómo correrlo

```bash
docker compose up -d --wait
./correr.sh 2026-09-30 prueba
./ver-batch.sh
```

## Día 1 · Mi primer Job

### Boleto de salida

1. **¿Qué diferencia hay entre un proceso batch y la API REST de la Semana 3? Da dos.**
   - **Interacción y flujo de ejecución:** Una API REST es interactiva y atiende peticiones síncronas o inmediatas de usuarios/clientes (petición HTTP y respuesta inmediata por transacción individual). Un proceso batch corre de forma automatizada, desatendida y programada en segundo plano (offline), procesando colecciones masivas de datos de principio a fin sin interacción de usuarios.
   - **Volumen de datos y transaccionalidad:** Una API REST está pensada para transacciones unitarias de baja latencia; un proceso batch está diseñado para procesar grandes volúmenes de datos mediante secuencias estructuradas de pasos (Steps) o bloques transaccionales (Chunks) con tolerancia a fallos y auditoría de contadores.

2. **¿Qué es un Job, qué es un Step y qué es un Tasklet?**
   - **Job:** Es el proceso por lotes completo; el contenedor de más alto nivel que orquesta la ejecución de uno o varios Steps en una secuencia definida.
   - **Step:** Es una fase o unidad atómica de ejecución dentro de un Job que encapsula una tarea específica.
   - **Tasklet:** Es una implementación de Step enfocada en realizar una tarea simple y única (como validar un archivo o emitir un mensaje) ejecutándose dentro de una transacción y finalizando al devolver `RepeatStatus.FINISHED`.

3. **Con tus tablas: ¿qué diferencia hay entre una JobInstance y una JobExecution?**
   - **JobInstance (`BATCH_JOB_INSTANCE`):** Es la representación lógica y única de un Job con sus parámetros identificadores específicos (por ejemplo, el cierre del `2026-09-28`). Solo existe un registro por cada combinación única de parámetros identificadores.
   - **JobExecution (`BATCH_JOB_EXECUTION`):** Es cada intento físico de ejecución de una `JobInstance`. Contiene información sobre la corrida particular (estado `COMPLETED`/`FAILED`, hora de inicio, hora de fin, código de salida). Una `JobInstance` puede tener varias `JobExecution` si las primeras fallaron.

4. **¿Por qué Spring Batch no deja correr dos veces el cierre del 28?**
   - Porque la `JobInstance` con fecha `2026-09-28` ya tiene una `JobExecution` previa con estado `COMPLETED`. Spring Batch implementa esta regla de negocio e idempotencia para proteger al banco: evita procesar dos veces el mismo cierre diario o duplicar movimientos contables. Al intentar correrlo nuevamente, lanza `JobInstanceAlreadyCompleteException`.

5. **(MP-4, paso 6) Si mañana llega el archivo del 25 y corres otra vez el cierre del 25, ¿será otra instancia u otra ejecución de la misma? ¿Por qué lo crees?**
   - Será **otra ejecución (`JobExecution`) de la misma instancia (`JobInstance`)**.
   - **¿Por qué?** Porque el parámetro identificador sigue siendo `fecha=2026-12-25`, por lo que la `JobInstance` ya existe en la tabla `BATCH_JOB_INSTANCE` (`JOB_INSTANCE_ID = 4`). Dado que la ejecución anterior (`JOB_EXECUTION_ID = 4`) terminó en `FAILED`, Spring Batch permite reintentar la misma instancia lógica registrando una nueva fila en `BATCH_JOB_EXECUTION` vinculada a la misma instancia.

## Día 2 · El primer chunk

### Boleto de salida

1. **¿Qué diferencia hay entre un step de tipo Tasklet y uno de tipo chunk?**
   - Un **Tasklet** ejecuta una tarea simple e indivisible (como verificar si existe un archivo o realizar una consulta/mantenimiento puntual) dentro de una transacción y termina devolviendo `RepeatStatus.FINISHED`.
   - Un step de tipo **chunk** está diseñado para procesar grandes volúmenes de datos en fragmentos transaccionales repetitivos, leyendo elemento por elemento (`ItemReader`), procesándolos o limpiándolos opcionalmente (`ItemProcessor`) y escribiéndolos en bloques (`ItemWriter`) delimitados por un intervalo de confirmación (`commit-interval`).

2. **¿Qué hace cada una de las tres piezas de un chunk? ¿Cuál es opcional?**
   - **ItemReader (Lector):** Lee datos de entrada secuencialmente (desde un archivo, base de datos, etc.) uno por uno hasta completar el tamaño del chunk o agotar los datos.
   - **ItemProcessor (Procesador):** Recibe cada elemento leído para transformarlo, limpiarlo o validarlo antes de enviarlo a persistencia. **Es la pieza opcional**; si no se define, los elementos pasan tal como se leyeron directamente al escritor.
   - **ItemWriter (Escritor):** Recibe la lista completa acumulada en el chunk procesado y la escribe en bloque (por ejemplo, múltiples `INSERT` por JDBC en una sola llamada por lote).

3. **Con 45 movimientos y chunks de 10, ¿cuántos commits habría? ¿Y con chunks de 50?**
   - **Con chunks de 10:** Habría **5 commits** ($10 + 10 + 10 + 10 + 5$). Los primeros cuatro chunks confirman 10 registros cada uno y el último confirma los 5 restantes.
   - **Con chunks de 50:** Habría **1 commit**, ya que los 45 registros entran en un único chunk que se confirma en una sola transacción.

4. **¿Por qué el Escritor recibe el chunk completo y no un movimiento a la vez?**
   - Por rendimiento y optimización de base de datos. Enviar registros individualmente implica múltiples viajes de red (round-trips) y apertura/confirmación repetitiva de transacciones. Al recibir la lista completa, el escritor ejecuta sentencias por lotes (`batch inserts`), reduciendo la sobrecarga de I/O y asegurando que todo el bloque se confirme de forma atómica en un único `commit`.

5. **Mi predicción de la MP-3, paso 1: ¿qué habría pasado sin el Procesador?**
   - En la base de datos se habrían almacenado registros heterogéneos y con espacios (como `"deposito"`, `"Retiro"` o `" RETIRO"`). Al consultar con collation binario (`utf8mb4_0900_bin`), habrían aparecido múltiples grupos divididos en lugar de solo dos, y al realizar cálculos o sumas de saldos posteriores, los tipos con espacios no coincidirían con las reglas del banco, corrompiendo los balances contables.

## Día 3 · Parámetros, fallas y reinicio

### Boleto de salida

1. **¿Qué diferencia hay entre una JobInstance y una JobExecution? Usa como ejemplo el cierre del 25.**
   - Una **JobInstance** representa la definición lógica única del trabajo parametrizado (por ejemplo, el cierre del día `2026-12-25` en `BATCH_JOB_INSTANCE`). Solo hay una instancia para esa fecha independientemente de cuántas veces se intente.
   - Una **JobExecution** representa cada intento físico e individual de ejecutar esa instancia (`BATCH_JOB_EXECUTION`). En el caso del cierre del 25, la primera ejecución falló (`FAILED`) porque el archivo no había llegado, y cuando el archivo llegó, el segundo intento fue una nueva ejecución (`COMPLETED`) vinculada exactamente a la misma instancia. Una instancia no se considera completa hasta que una de sus ejecuciones finaliza en `COMPLETED`.

2. **¿En qué caso Spring Batch se niega a correr un cierre, y en qué caso lo reinicia?**
   - **Se niega (`JobInstanceAlreadyCompleteException`):** Cuando se intenta ejecutar con los mismos parámetros identificadores (misma fecha) de una `JobInstance` que ya tiene una ejecución previa en estado `COMPLETED`. Esto protege al banco contra duplicación de procesos y transacciones ya liquidadas.
   - **Lo reinicia:** Cuando se ejecuta con los mismos parámetros identificadores de una `JobInstance` cuya última ejecución terminó en estado no completado (como `FAILED`). En ese caso, Spring Batch crea una nueva `JobExecution` para retomar el trabajo.

3. **En el reinicio del día 5, ¿por qué el step de carga leyó 10 movimientos y no 20?**
   - Porque Spring Batch almacena el estado del progreso en sus tablas de metadatos (`BATCH_STEP_EXECUTION_CONTEXT`). En el primer intento fallido, el primer chunk de 10 movimientos ya se había confirmado exitosamente (`COMMIT_COUNT = 1`) antes de que fallara el segundo chunk en la línea 16. Al reiniciar, Spring Batch sabe que los primeros 10 ya están guardados en MySQL, por lo que el `ItemReader` salta automáticamente esos 10 y comienza a leer desde el movimiento 11, leyendo únicamente los 10 restantes para completar los 20 sin duplicar datos.

4. **¿Qué diferencia hay entre un movimiento filtrado y uno omitido?**
   - **Filtrado (`FILTER_COUNT`):** Es una decisión intencional de la lógica de negocio aplicada en el `ItemProcessor` al retornar `null` (por ejemplo, descartar un tipo no contemplado como `TRANSFERENCIA` o `PAGO`). No se considera un error ni genera excepciones; el registro se lee pero deliberadamente no se envía al escritor.
   - **Omitido (`SKIP_COUNT`):** Es una tolerancia técnica ante errores o excepciones que impiden procesar un registro (por ejemplo, un `FlatFileParseException` por un formato corrupto con letras en montos numéricos o delimitadores incorrectos). En lugar de detener el batch, Spring Batch atrapa la excepción permitida y descarta ese renglón, acumulándolo en el contador de skips hasta alcanzar un límite predefinido (`skipLimit`).

5. **¿Por qué importa el código de salida, si el estado ya queda en las tablas?**
   - Porque los procesos por lotes en producción son ejecutados de manera automatizada y desatendida por planificadores de tareas del sistema operativo o software empresarial (como Control-M, cron o Kubernetes Jobs). Estos orquestadores no consultan las tablas internas de la base de datos de Spring Batch; únicamente evalúan el código de salida numérico del proceso (`exit code`). Un código `0` indica éxito (`COMPLETED`), mientras que un código distinto de cero (como `5` para `FAILED`) activa de inmediato alertas operativas, reintentos o detención de tareas dependientes en la cadena batch del banco.
