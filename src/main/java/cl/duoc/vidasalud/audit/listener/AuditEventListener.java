package cl.duoc.vidasalud.audit.listener;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

import cl.duoc.vidasalud.audit.event.AuditEvent;
import cl.duoc.vidasalud.audit.service.AuditTimelineService;

/**
 * Consumer group "auditoria-timeline": lee el topico de auditoria y arma la
 * linea de tiempo que consulta el frontend.
 */
@Component
public class AuditEventListener extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(AuditEventListener.class);

    private final EventValidator validator;
    private final AuditTimelineService timeline;

    public AuditEventListener(EventValidator validator, AuditTimelineService timeline) {
        this.validator = validator;
        this.timeline = timeline;
    }

    /**
     * Este servicio guarda su estado solo en memoria. Un grupo que ya confirmo
     * offsets seguiria desde ahi tras un reinicio y el estado quedaria vacio;
     * por eso, al asignarse cada particion se relee desde el inicio. Es seguro
     * repetir eventos: el servicio descarta los duplicados por eventId.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        assignments.keySet().forEach(tp -> callback.seekToBeginning(tp.topic(), tp.partition()));
        log.info("Reconstruyendo estado: releyendo la auditoria desde el inicio ({} particion(es) asignada(s))",
                assignments.size());
    }

    @KafkaListener(
            id = "auditoria-timeline",
            topics = "${vidasalud.kafka.topics.audit-events}",
            groupId = "${vidasalud.kafka.groups.timeline}",
            properties = "spring.json.value.default.type=cl.duoc.vidasalud.audit.event.AuditEvent")
    public void onAuditEvent(ConsumerRecord<String, AuditEvent> record) {
        AuditEvent event = validator.validate(record.value(), record.offset());

        if (timeline.add(event)) {
            log.info("Linea de tiempo: {} {} (particion={}, offset={})",
                    event.accion(), event.correlationId(), record.partition(), record.offset());
        } else {
            log.info("Evento de auditoria duplicado descartado: {}", event.eventId());
        }
    }
}
