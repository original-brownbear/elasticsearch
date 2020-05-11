package org.elasticsearch.action.admin.cluster.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Semaphore;

public abstract class TransportSnapshotApiAction<Request extends
        MasterNodeRequest<Request>, Response extends ActionResponse> extends TransportMasterNodeAction<Request, Response> {

    private final Semaphore limiter;

    protected TransportSnapshotApiAction(String actionName, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters, Writeable.Reader<Request> request,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
        limiter = new Semaphore(5);
    }

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (limiter.tryAcquire()) {
            boolean success = false;
            try {
                executeApiAction(task, request, ActionListener.runBefore(listener, limiter::release));
                success = true;
            } finally {
                if (success == false) {
                    limiter.release();
                }
            }
        }
    }

    protected abstract void executeApiAction(Task task, Request request, ActionListener<Response> listener);
}
