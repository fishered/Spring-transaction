package dev.cluster.tx.sample;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.core.SagaParticipant;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Component
@SagaParticipant("inventory")
@ConditionalOnProperty(name = "sample.role", havingValue = "participant")
class SampleParticipant implements ParticipantAction {
    private final JdbcTemplate jdbc;

    SampleParticipant(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public ActionResult execute(SagaCommand command) {
        if ("reject".equals(command.payload())) return ActionResult.REJECTED;
        int changed = jdbc.update("UPDATE stock SET quantity = quantity - 1 WHERE id = 1 AND quantity > 0");
        return changed == 1 ? ActionResult.APPLIED : ActionResult.REJECTED;
    }

    @Override
    public void compensate(SagaCommand command) {
        jdbc.update("UPDATE stock SET quantity = quantity + 1 WHERE id = 1");
    }

    @RestController
    @ConditionalOnProperty(name = "sample.role", havingValue = "participant")
    static class StockController {
        private final JdbcTemplate jdbc;

        StockController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

        @GetMapping("/stock")
        int stock() { return jdbc.queryForObject("SELECT quantity FROM stock WHERE id = 1", Integer.class); }
    }
}
