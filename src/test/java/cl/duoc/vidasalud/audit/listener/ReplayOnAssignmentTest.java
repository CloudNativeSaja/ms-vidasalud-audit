package cl.duoc.vidasalud.audit.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import cl.duoc.vidasalud.audit.service.AuditTimelineService;

/**
 * La linea de tiempo vive solo en memoria: al reiniciar, el grupo ya tiene
 * offsets confirmados y, si no releyera el topico, la auditoria quedaria vacia.
 */
class ReplayOnAssignmentTest {

    @Test
    void alAsignarseParticionesRelaeDesdeElInicio() {
        AuditEventListener listener = new AuditEventListener(
                mock(EventValidator.class), mock(AuditTimelineService.class));
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);

        listener.onPartitionsAssigned(Map.of(
                new TopicPartition("vidasalud.audit.events", 1), 3L), callback);

        verify(callback).seekToBeginning("vidasalud.audit.events", 1);
        verifyNoMoreInteractions(callback);
    }

    @Test
    void elGrupoQueTransformaEventosNoRelaeAlReiniciar() {
        // AppointmentEventListener (grupo "auditoria") publica en Kafka: releerlo
        // en cada reinicio duplicaria los eventos de auditoria.
        org.junit.jupiter.api.Assertions.assertFalse(
                org.springframework.kafka.listener.ConsumerSeekAware.class
                        .isAssignableFrom(AppointmentEventListener.class));
    }
}
