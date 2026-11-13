package example;

import dev.cluster.tx.core.SagaClient;
import dev.cluster.tx.http.HttpSagaAction;

public final class ConsumerSmoke {
    private final SagaClient client;
    private final HttpSagaAction httpAction;

    public ConsumerSmoke(SagaClient client, HttpSagaAction httpAction) {
        this.client = client;
        this.httpAction = httpAction;
    }

    public SagaClient client() {
        return client;
    }

    public HttpSagaAction httpAction() {
        return httpAction;
    }
}
