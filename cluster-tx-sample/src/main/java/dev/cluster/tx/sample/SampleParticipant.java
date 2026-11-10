package dev.cluster.tx.sample;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.http.ExecutionReply;
import dev.cluster.tx.http.OperationReply;
import dev.cluster.tx.http.ParticipantHttpHandler;
import dev.cluster.tx.jdbc.ParticipantExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "sample.role", havingValue = "participant")
class SampleParticipant {
    private final ParticipantHttpHandler handler;
    private final JdbcTemplate jdbc;

    SampleParticipant(ParticipantExecutor executor, JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.handler = new ParticipantHttpHandler(executor, new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) {
                if ("reject".equals(command.payload())) return ActionResult.REJECTED;
                int changed = jdbc.update("UPDATE stock SET quantity = quantity - 1 WHERE id = 1 AND quantity > 0");
                return changed == 1 ? ActionResult.APPLIED : ActionResult.REJECTED;
            }
            @Override public void compensate(SagaCommand command) {
                jdbc.update("UPDATE stock SET quantity = quantity + 1 WHERE id = 1");
            }
        });
    }

    @PostMapping("/tx/inventory/execute")
    ExecutionReply execute(@RequestBody SagaCommand command) { return handler.execute(command); }

    @GetMapping("/tx/inventory/operations/{operationId}")
    OperationReply query(@PathVariable String operationId) { return handler.query(operationId); }

    @PostMapping("/tx/inventory/compensate")
    OperationReply compensate(@RequestBody SagaCommand command) { return handler.compensate(command); }

    @GetMapping("/stock")
    int stock() { return jdbc.queryForObject("SELECT quantity FROM stock WHERE id = 1", Integer.class); }
}
