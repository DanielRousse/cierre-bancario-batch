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
