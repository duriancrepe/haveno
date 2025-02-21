/*
 * This file is part of Haveno.
 *
 * Haveno is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * Haveno is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Haveno. If not, see <http://www.gnu.org/licenses/>.
 */

package bisq.network.p2p.storage.persistence;

import bisq.common.proto.persistable.PersistableEnvelope;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

/**
 * Used for handling data from resource files.
 */
@Slf4j
public class ResourceDataStoreService {
    private final List<StoreService<? extends PersistableEnvelope>> services;

    @Inject
    public ResourceDataStoreService() {
        services = new ArrayList<>();
    }

    public void addService(StoreService<? extends PersistableEnvelope> service) {
        services.add(service);
    }

    public void readFromResources(String postFix, Runnable completeHandler) {
        if (services.isEmpty()) {
            completeHandler.run();
            return;
        }
        AtomicInteger remaining = new AtomicInteger(services.size());
        services.forEach(service -> {
            service.readFromResources(postFix, () -> {
                if (remaining.decrementAndGet() == 0) {
                    completeHandler.run();
                }
            });
        });
    }

    // Uses synchronous execution on the userThread. Only used by tests. The async methods should be used by app code.
    @VisibleForTesting
    public void readFromResourcesSync(String postFix) {
        services.forEach(service -> service.readFromResourcesSync(postFix));
    }
}
