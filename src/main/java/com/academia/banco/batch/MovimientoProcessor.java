package com.academia.banco.batch;

import com.academia.banco.model.Movimiento;
import java.math.BigDecimal;
import org.springframework.batch.infrastructure.item.ItemProcessor;

// El Procesador: recibe UN movimiento como lo leyó el Lector y devuelve el que se va a escribir.
public class MovimientoProcessor implements ItemProcessor<Movimiento, Movimiento> {

    private static final BigDecimal LIMITE_MONTO = new BigDecimal("10000.00");

    @Override
    public Movimiento process(Movimiento movimiento) {
        String tipo = movimiento.tipo().trim().toUpperCase();   // " retiro" → "RETIRO"
        if (!tipo.equals("DEPOSITO") && !tipo.equals("RETIRO")) {
            return null;                                         // null = «este no se escribe» (se FILTRA)
        }
        if (movimiento.monto().compareTo(LIMITE_MONTO) > 0) {
            return null;                                         // filtro: montos mayores a 10,000 a revisión manual
        }
        return new Movimiento(movimiento.cuenta().trim(), tipo, movimiento.monto());
    }
}
