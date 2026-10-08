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

## Día 4 · De MySQL a MongoDB

### Boleto de salida

1. **¿Qué hace cada uno de los tres steps de tu Job, y de qué tipo es cada uno?**
   - **`verificarArchivoStep` (tipo Tasklet):** Valida que exista en el sistema de archivos el archivo CSV del día (`datos/movimientos-<fecha>.csv`) y cuenta los movimientos declarados. Si el archivo no existe, lanza una excepción y detiene el Job de inmediato antes de iniciar la carga.
   - **`cargarMovimientosStep` (tipo Chunk):** Lee los movimientos desde el archivo CSV con `FlatFileItemReader`, los procesa y filtra con `MovimientoProcessor` (`ItemProcessor`), y los inserta en la tabla `movimiento` de MySQL por bloques de 10 en 10 con `JdbcBatchItemWriter`. Cuenta con tolerancia a fallas (`faultTolerant`, `skip(FlatFileParseException.class)`, `skipLimit(3)`).
   - **`publicarSaldosStep` (tipo Chunk, sin Procesador):** Lee desde MySQL mediante `JdbcCursorItemReader` el saldo calculado (depósitos menos retiros) y el número de movimientos agrupados por cuenta (`GROUP BY cuenta`), y escribe/actualiza los documentos consolidados de 3 en 3 en la colección `saldos` de MongoDB usando `MongoItemWriter`.

2. **¿Por qué el cierre del 9 no duplicó los saldos, y el del 10 (sin `@Id`) sí?**
   - **Cierre del 9 (con `@Id`):** La anotación `@Id` en `SaldoCuenta` establece que el número de cuenta es el `_id` único del documento en MongoDB. Al escribir con `MongoItemWriter`, MongoDB ejecuta una operación de reemplazo/upsert basada en ese `_id`: si el documento de la cuenta ya existe, lo actualiza con los saldos nuevos en vez de duplicarlo, manteniendo exactamente 15 documentos.
   - **Cierre del 10 (sin `@Id`):** Al remover `@Id`, MongoDB ya no utiliza la cuenta como identificador del documento y genera automáticamente un nuevo `ObjectId` único y aleatorio para cada registro en cada ejecución. Dado que ese `ObjectId` nunca antes existió, MongoDB inserta un documento adicional por cada cuenta, pasando de 15 a 30 documentos duplicados.

3. **Al reiniciar el cierre del 11, ¿por qué no se cargó otra vez el archivo?**
   - Porque Spring Batch consulta el historial de ejecuciones en sus tablas de metadatos (`BATCH_STEP_EXECUTION`). Al detectar que en el primer intento fallido los steps `verificarArchivoStep` y `cargarMovimientosStep` terminaron exitosamente con estado `COMPLETED`, el framework determina que dichos pasos ya están completos (`Step already complete or not restartable, so no action to execute`). Por lo tanto, no los vuelve a ejecutar, evitando cargar nuevamente los 15 movimientos a MySQL y retomando el flujo directamente en el paso que falló (`publicarSaldosStep`).

4. **¿Qué diferencia hay entre `spring-boot-starter-data-mongodb` y «Spring Batch MongoDB» (`batch-data-mongodb`)?**
   - **`spring-boot-starter-data-mongodb`:** Es la dependencia de Spring Data MongoDB utilizada por la aplicación para interactuar con MongoDB como base de datos de negocio (escribir y consultar los documentos de saldos del banco mediante `MongoTemplate` y `MongoItemWriter`), manteniendo la auditoría interna de Spring Batch en MySQL.
   - **«Spring Batch MongoDB» (`batch-data-mongodb`):** Es una dependencia diseñada para que el repositorio interno de metadatos de Spring Batch (`JobRepository`) almacene las propias tablas del framework (`BATCH_JOB_INSTANCE`, `BATCH_JOB_EXECUTION`, etc.) en colecciones de MongoDB en vez de un motor relacional como MySQL (lo cual requiere que MongoDB opere con réplicas configuradas para soportar transacciones multi-documento).

## Lo que aprendí esta semana

Un proceso batch es una aplicación automatizada y desatendida que procesa grandes volúmenes de datos históricos o acumulados de manera secuencial, sin requerir interacción en tiempo real por parte de un usuario. Un Job es el contenedor principal que orquesta un flujo de trabajo compuesto por uno o varios Steps, los cuales pueden ser de tipo Tasklet (tareas indivisibles y puntuales) o de tipo Chunk (fragmentos transaccionales estructurados en ItemReader, ItemProcessor e ItemWriter con intervalos de confirmación parciales). Cuando ocurre una falla, Spring Batch detiene la ejecución de forma ordenada y registra detalladamente el estado, los contadores de progreso y el mensaje del error en sus tablas de metadatos en la base de datos relacional. Gracias a esta arquitectura transaccional, Spring Batch garantiza idempotencia y recuperación inteligente: al solucionar el problema y relanzar el Job con los mismos parámetros identificadores, el sistema omite los Steps que ya concluyeron en `COMPLETED` y reanuda el procesamiento exactamente desde el último chunk confirmado, asegurando que no se dupliquen registros ni se corrompa la contabilidad del banco.

## Reto opcional · Cuentas sobregiradas

- **Resultado de la ejecución (`2026-10-22`):**
  - `publicarSaldosStep`: `READ_COUNT = 15`, `FILTER_COUNT = 6`, `WRITE_COUNT = 9`, `COMMIT_COUNT = 5`.
  - El `ItemProcessor` identificó las 6 cuentas con saldo negativo y devolvió `null`, filtrándolas para que el escritor solo recibiera las 9 cuentas con saldo positivo o cero.
- **¿Qué pasa con los documentos viejos en MongoDB y qué harías?**
  - Dado que filtrar (`return null`) simplemente descarta el registro en el escritor y no emite operaciones de borrado (`DELETE`), los documentos de esas 6 cuentas sobregiradas que ya se habían guardado en cierres de días anteriores siguen existiendo en MongoDB con saldos desactualizados.
  - **Solución propuesta:**
    1. **Tasklet de limpieza o reemplazo total:** Agregar un step Tasklet previo a la publicación que limpie la colección (`mongoTemplate.dropCollection("saldos")`), o ejecutar un `deleteMany` de cuentas que ya no tienen saldo activo.
    2. **Marcado explícito (Flag de negocio):** En lugar de descartar con `null`, actualizar el documento en MongoDB con un campo de control como `sobregirada: true` o `estado: "SOBREGIRADA"`, permitiendo a los sistemas consumidores y a la app bancaria conocer el estado contable actual en vez de consultar datos obsoletos.
