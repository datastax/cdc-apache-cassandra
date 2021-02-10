/*
 * Copyright (C) 2020 Strapdata SAS (support@strapdata.com)
 *
 * The Elassandra-Operator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The Elassandra-Operator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with the Elassandra-Operator.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.quasar.Status;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import javax.inject.Inject;

/**
 * K8s readiness endpoint
 */
@Controller("/ready")
public class ReadinessController {

    @Inject
    QuasarClusterManager clusterManager;

    /**
     * Returns 200 if the node is RUNNING, otherwise 404.
     * @return The readiness of the node.
     */
    @Get("/")
    public HttpStatus index() {
        return Status.RUNNING.equals(this.clusterManager.stateAtomicReference.get().getStatus())
                ? HttpStatus.OK
                : HttpStatus.NOT_FOUND;
    }

}
